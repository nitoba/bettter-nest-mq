import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { MemoryJobStore, Queue, QueueControls, makeQueueName } from 'better-effect-mq'
import { Result } from 'better-result'
import { flowReadStore, type FlowJobReads } from '../../src/engine/flow-read-store.ts'
import { controlledStore } from '../../src/engine/controlled-store.ts'

function referenceReads(store: ReturnType<typeof MemoryJobStore.make>): FlowJobReads {
  return {
    getJob: async (jobId) => store.getJob({ jobId }),
    getAttempts: async (jobId) => store.getAttempts({ jobId }),
    async counts(request) {
      const result = await store.counts(request)
      if (Result.isError(result)) return result
      return Result.ok({ ...result.value, waitingChildren: 0 })
    },
    recoveryIds: async () => []
  }
}

test('a store without the v2 read companion retains its original identity', () => {
  const store = MemoryJobStore.make()
  expect(flowReadStore(store, undefined, () => undefined)).toBe(store)
})

test('typed read delegation preserves the real controlled extension and method receivers', async () => {
  const store = MemoryJobStore.make()
  const view = flowReadStore(store, referenceReads(store), () => undefined)
  const controls = controlledStore(view)
  assert.ok(controls)
  const queue = makeQueueName('delegated-controls')
  assert.ok(Result.isOk(queue))
  const policy = QueueControls.registry({
    group: 'read-companion-test',
    controls: [QueueControls.define(Queue.define(queue.value), { globalConcurrency: 2 })]
  })
  const created = await controls.reconcile(policy, { removal: 'ignore' })
  assert.ok(Result.isOk(created))
  const direct = await store.getControls({ queue: queue.value })
  const delegated = await controls.get(queue.value)
  assert.ok(Result.isOk(direct) && Result.isOk(delegated))
  expect(delegated.value).toEqual(direct.value)
  expect(delegated.value?.globalConcurrency).toBe(2)
  expect(view.descriptor).toBe(store.descriptor)
  const counts = await view.counts()
  assert.ok(Result.isOk(counts))
  expect(counts.value.total).toBe(0)
})
