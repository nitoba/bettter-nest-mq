import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Test } from '@nestjs/testing'
import { Layer } from 'better-effect'
import {
  MemoryJobStore,
  Queue as EngineQueue,
  QueueControls as EngineControls,
  makeQueueName
} from 'better-effect-mq'
import { Result } from 'better-result'
import { z } from 'zod'
import {
  Job,
  MqModule,
  MqQueueControlsService,
  Queue,
  QueueControls,
  QueueControlsException,
  QueueService
} from '../../src/index.ts'
import { defineConnection } from '../../src/engine/connection-definition.ts'
import { controlReferenceStore } from '../fixtures/control-store.ts'

class Jobs extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({ payload: z.string(), result: z.string() })
}
@Queue({ name: 'first', connection: 'first' })
@QueueControls({ globalConcurrency: 2 })
class FirstQueue extends Jobs {}
@Queue({ name: 'second', connection: 'second' })
@QueueControls({ globalConcurrency: 2 })
class SecondQueue extends Jobs {}

function connection(store: ReturnType<typeof MemoryJobStore.make>) {
  return defineConnection(
    { adapter: 'memory', ownership: 'borrowed', boundary: store, scope: 'safety-test' },
    (token) => ({ layer: Layer.succeed(token, store) })
  )
}

function phase(expected: QueueControlsException['phase']) {
  return (error: Error): boolean => {
    assert.ok(error instanceof QueueControlsException)
    assert.equal(error.phase, expected)
    return true
  }
}

test('raw memory capabilities are not promoted to distributed guarantees by production code', async () => {
  const store = MemoryJobStore.make()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({
        connections: { first: connection(store) },
        controls: { mode: 'reconcile' }
      }),
      MqModule.forFeature([FirstQueue])
    ]
  }).compile()
  await assert.rejects(app.init(), phase('capability'))
  await assert.rejects(app.close(), QueueControlsException)
})

test('all adapters are checked before any policy write, not one write at a time', async () => {
  const first = controlReferenceStore()
  const second = MemoryJobStore.make()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({
        connections: { first: connection(first), second: connection(second) },
        controls: { mode: 'reconcile' }
      }),
      MqModule.forFeature([FirstQueue, SecondQueue])
    ]
  }).compile()
  await assert.rejects(app.init(), phase('capability'))
  const queue = makeQueueName('first')
  if (Result.isError(queue)) throw queue.error
  const persisted = await first.getControls({ queue: queue.value })
  if (Result.isError(persisted)) throw persisted.error
  expect(persisted.value).toBeUndefined()
  await assert.rejects(app.close(), QueueControlsException)
})

test('matching groups with different limits fail validation and only explicit reconciliation changes revision', async () => {
  const store = controlReferenceStore()
  const seeded = await store.reconcile(
    EngineControls.registry({
      group: 'nestjs/queue-controls',
      controls: [EngineControls.define(EngineQueue.define('first'), { globalConcurrency: 7 })]
    }),
    { removal: 'ignore' }
  )
  if (Result.isError(seeded)) throw seeded.error
  const readOnly = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { first: connection(store) } }),
      MqModule.forFeature([FirstQueue])
    ]
  }).compile()
  await assert.rejects(readOnly.init(), phase('drift'))
  await assert.rejects(readOnly.close(), QueueControlsException)
  const deploy = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({
        connections: { first: connection(store) },
        controls: { mode: 'reconcile' }
      }),
      MqModule.forFeature([FirstQueue])
    ]
  }).compile()
  try {
    await deploy.init()
    expect(await deploy.get(MqQueueControlsService).get(FirstQueue)).toMatchObject({
      globalConcurrency: 2,
      revision: 2
    })
  } finally {
    await deploy.close()
  }
})

test('omitting a policy from a deployment never disables a previously persisted queue', async () => {
  const store = controlReferenceStore()
  const seeded = await store.reconcile(
    EngineControls.registry({
      group: 'nestjs/queue-controls',
      controls: [EngineControls.define(EngineQueue.define('unlisted'), { globalConcurrency: 1 })]
    }),
    { removal: 'ignore' }
  )
  if (Result.isError(seeded)) throw seeded.error
  const deploy = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({
        connections: { first: connection(store) },
        controls: { mode: 'reconcile' }
      }),
      MqModule.forFeature([FirstQueue])
    ]
  }).compile()
  try {
    await deploy.init()
    const queue = makeQueueName('unlisted')
    if (Result.isError(queue)) throw queue.error
    const persisted = await store.getControls({ queue: queue.value })
    if (Result.isError(persisted)) throw persisted.error
    expect(persisted.value).toMatchObject({ enabled: true, revision: 1, globalConcurrency: 1 })
  } finally {
    await deploy.close()
  }
})
