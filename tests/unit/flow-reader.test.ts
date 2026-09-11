import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Effect } from 'better-effect'
import { Flow as EngineFlow } from 'better-effect-mq'
import { Result } from 'better-result'
import { z } from 'zod'
import { Job, Queue, QueueService, flowJob, getQueueDefinition, MqFlowException } from '../../src/index.ts'
import { compileJob } from '../../src/engine/job-compiler.ts'
import { FlowResultReader } from '../../src/engine/flow-results.ts'

@Queue({ name: 'reader' })
class ReaderQueue extends QueueService {
  @Job({ name: 'parent', version: 1 }) readonly parent = this.job({ payload: z.string(), result: z.string() })
  @Job({ name: 'child', version: 1 }) readonly child = this.job({ payload: z.string(), result: z.string() })
}
const parent = flowJob(ReaderQueue, 'parent')
const child = flowJob(ReaderQueue, 'child')
function compiled() {
  const jobs = getQueueDefinition(new ReaderQueue()).jobs
  const parentJob = jobs.find((job) => job.property === 'parent')
  const childJob = jobs.find((job) => job.property === 'child')
  assert.ok(parentJob && childJob)
  const p = { registered: parentJob, compiled: compileJob(parentJob) }
  const c = { registered: childJob, compiled: compileJob(childJob) }
  return {
    owner: 'reader', options: { name: 'reader', parent, children: [child], onChildFailure: 'continue' as const },
    parent: p, children: new Map([[child, c]]),
    definition: EngineFlow.define('reader', { parent: p.compiled, children: [c.compiled], onChildFailure: 'continue' }),
    fanOutMethod: 'split', collectMethod: 'finish'
  }
}
function readerFixture() {
  let reads = 0
  const gate = Promise.withResolvers<void>()
  const reader = new FlowResultReader(compiled(), {
    counts: { pending: 0, completed: 0, failed: 0, cancelled: 0 },
    page: () => Effect.fn(async function* () {
      reads += 1
      const page = yield* Result.await(gate.promise.then(() => Result.ok({ items: [], nextCursor: undefined })))
      return Result.ok(page)
    }),
    all: () => { throw new Error('The phase-owned reader must use bounded pages, not upstream all()') },
    forEach: () => { throw new Error('The phase-owned reader must use bounded pages, not upstream forEach()') }
  })
  return { reader, gate, reads: () => reads }
}

test('unissued flow cursors reject before querying rather than restarting pagination', async () => {
  const fixture = readerFixture()
  fixture.gate.resolve()
  await assert.rejects(fixture.reader.page(child, { cursor: 'invented' }), MqFlowException)
  expect(fixture.reads()).toBe(0)
  await fixture.reader.close()
})
test('closing a phase reader drains an already admitted read and rejects new reads', async () => {
  const fixture = readerFixture()
  const page = fixture.reader.page(child)
  let closed = false
  const closing = fixture.reader.close().then(() => { closed = true })
  await assert.rejects(fixture.reader.page(child), MqFlowException)
  expect(closed).toBe(false)
  fixture.gate.resolve()
  await page
  await closing
  expect(closed).toBe(true)
})
