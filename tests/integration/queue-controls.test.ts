import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Inject, Injectable } from '@nestjs/common'
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
  ContractDefinitionException,
  Job,
  JobData,
  MqModule,
  MqQueueControlsService,
  Process,
  Queue,
  QueueControls,
  QueueControlsException,
  QueueService,
  Worker,
  type MqModuleOptions
} from '../../src/index.ts'
import { defineConnection } from '../../src/engine/connection-definition.ts'

@Queue({ name: 'limited', connection: 'primary' })
@QueueControls({ globalConcurrency: 2, perKeyConcurrency: 1 })
class LimitedQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({
    payload: z.object({ tenant: z.string(), value: z.int() }),
    result: z.int(),
    dispatchKey: (payload) => payload.tenant
  })
  @Job({ name: 'manual-key', version: 1 })
  readonly manual = this.job({ payload: z.string(), result: z.string() })
}

function sharedStore() {
  const store = MemoryJobStore.make()
  let acquired = 0
  let released = 0
  const connection = defineConnection(
    { adapter: 'memory', ownership: 'borrowed', boundary: store, scope: 'shared-control-test' },
    (token) => ({
      layer: Layer.scoped(
        token,
        () => {
          acquired += 1
          return store
        },
        () => {
          released += 1
        }
      )
    })
  )
  return { store, connection, counts: () => ({ acquired, released }) }
}

async function createApp(
  fixture: ReturnType<typeof sharedStore>,
  controls?: MqModuleOptions['controls']
) {
  const options: MqModuleOptions =
    controls === undefined
      ? { connections: { primary: fixture.connection }, execution: { workers: false } }
      : { connections: { primary: fixture.connection }, execution: { workers: false }, controls }
  return Test.createTestingModule({
    imports: [MqModule.forRoot(options), MqModule.forFeature([LimitedQueue])]
  }).compile()
}

test('missing persisted controls fail default validation and clean up before any worker can run', async () => {
  const fixture = sharedStore()
  const app = await createApp(fixture)
  await assert.rejects(app.init(), QueueControlsException)
  expect(fixture.counts()).toEqual({ acquired: 1, released: 1 })
  await assert.rejects(app.close(), QueueControlsException)
})

test('explicit deployment reconciliation persists policy, keeps its revision, and allows read-only replicas', async () => {
  const fixture = sharedStore()
  const deploy = await createApp(fixture, { mode: 'reconcile' })
  let revision: number
  try {
    await deploy.init()
    const service = deploy.get(MqQueueControlsService)
    const first = await service.get(LimitedQueue)
    assert.ok(first)
    revision = first.revision
    expect(first).toMatchObject({
      queue: 'limited',
      connection: 'primary',
      enabled: true,
      globalConcurrency: 2,
      perKeyConcurrency: 1
    })
    expect(Object.isFrozen(first)).toBe(true)
    const reports = await service.reconcile()
    expect(reports[0]?.unchanged).toHaveLength(1)
    expect((await service.get(LimitedQueue))?.revision).toBe(revision)
  } finally {
    await deploy.close()
  }
  const replica = await createApp(fixture)
  try {
    await replica.init()
    expect((await replica.get(MqQueueControlsService).get(LimitedQueue))?.revision).toBe(revision)
    await assert.rejects(replica.get(MqQueueControlsService).reconcile(), QueueControlsException)
  } finally {
    await replica.close()
  }
})

test('policy drift and group ownership conflicts fail without rewriting persisted limits', async () => {
  const fixture = sharedStore()
  const registry = EngineControls.registry('foreign-owner', [
    EngineControls.define(EngineQueue.define('limited'), { globalConcurrency: 7 })
  ])
  const inserted = await fixture.store.reconcile(registry, { removal: 'ignore' })
  if (Result.isError(inserted)) throw inserted.error
  for (const mode of ['validate', 'reconcile'] as const) {
    const app = await createApp(fixture, { mode })
    await assert.rejects(app.init(), QueueControlsException)
    await assert.rejects(app.close(), QueueControlsException)
  }
  const name = makeQueueName('limited')
  if (Result.isError(name)) throw name.error
  const persisted = await fixture.store.getControls({ queue: name.value })
  if (Result.isError(persisted)) throw persisted.error
  expect(persisted.value).toMatchObject({
    group: 'foreign-owner',
    revision: 1,
    globalConcurrency: 7
  })
})

test('derived keys are used by prepare and batches, cannot be overridden, and missing manual keys fail before publication', async () => {
  const fixture = sharedStore()
  const app = await createApp(fixture, { mode: 'reconcile' })
  try {
    await app.init()
    const queue = app.get(LimitedQueue)
    expect((await queue.task.prepare({ tenant: 'A', value: 1 })).request.dispatchKey).toBe('A')
    await assert.rejects(
      queue.task.enqueue({ tenant: 'A', value: 1 }, { dispatchKey: 'B' }),
      ContractDefinitionException
    )
    await assert.rejects(queue.manual.enqueue('missing'), ContractDefinitionException)
    await assert.rejects(queue.manual.prepare('missing'), ContractDefinitionException)
    await assert.rejects(
      queue.task.enqueueMany([
        { payload: { tenant: 'A', value: 1 }, options: { jobId: 'no-partial-write' } },
        { payload: { tenant: 'B', value: 2 }, options: { dispatchKey: 'wrong' } }
      ]),
      ContractDefinitionException
    )
    expect(await queue.task.poll('no-partial-write')).toBeUndefined()
    const manual = await queue.manual.enqueue('valid', { dispatchKey: 'B' })
    expect((await queue.manual.poll(manual))?.state).toBe('waiting')
    expect(
      await queue.task.enqueueMany([
        { payload: { tenant: 'A', value: 1 } },
        { payload: { tenant: 'B', value: 2 } }
      ])
    ).toHaveLength(2)
  } finally {
    await app.close()
  }
})

@Injectable()
class Gates {
  readonly twoKeys = Promise.withResolvers<void>()
  readonly release = Promise.withResolvers<void>()
  readonly active = new Set<string>()
  readonly started: string[] = []
}
@Worker({ name: 'limited-worker', concurrency: 8, pollIntervalMs: 5 })
class LimitedWorker {
  constructor(@Inject(Gates) private readonly gates: Gates) {}
  @Process(LimitedQueue, 'task')
  async run(@JobData() payload: { tenant: string; value: number }) {
    assert.equal(this.gates.active.has(payload.tenant), false, 'Same-key work must not overlap')
    this.gates.active.add(payload.tenant)
    this.gates.started.push(payload.tenant)
    assert.ok(this.gates.active.size <= 2)
    if (this.gates.active.size === 2) this.gates.twoKeys.resolve()
    try {
      await this.gates.release.promise
      return payload.value
    } finally {
      this.gates.active.delete(payload.tenant)
    }
  }
}

test('the real supervisor reaches controlled claims and settles permits rather than falling back to plain claims', async () => {
  const fixture = sharedStore()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({
        connections: { primary: fixture.connection },
        controls: { mode: 'reconcile' }
      }),
      MqModule.forFeature([LimitedQueue])
    ],
    providers: [Gates, LimitedWorker]
  }).compile()
  const gates = app.get(Gates)
  try {
    await app.init()
    const queue = app.get(LimitedQueue)
    const ids = await queue.task.enqueueMany([
      { payload: { tenant: 'A', value: 1 } },
      { payload: { tenant: 'A', value: 2 } },
      { payload: { tenant: 'B', value: 3 } },
      { payload: { tenant: 'C', value: 4 } }
    ])
    await gates.twoKeys.promise
    expect(new Set(gates.started.slice(0, 2)).size).toBe(2)
    gates.release.resolve()
    expect(
      await Promise.all(
        ids.map((id) => queue.task.awaitResult(id, { timeoutMs: 2_000, pollIntervalMs: 5 }))
      )
    ).toEqual([1, 2, 3, 4])
  } finally {
    gates.release.resolve()
    await app.close()
  }
})
