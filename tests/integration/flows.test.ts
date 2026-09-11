import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  Flow,
  FanOut,
  Collect,
  FlowData,
  FlowChildren,
  flowJob,
  flowChildren,
  Job,
  JobData,
  MqModule,
  MqFlowsService,
  MqFlowException,
  Queue,
  QueueService,
  Process,
  Worker,
  type FlowResultsReader
} from '../../src/index.ts'
import { flowConnection } from '../fixtures/flow-connection.ts'

@Queue({ name: 'flows', connection: 'primary' })
class FlowQueue extends QueueService {
  @Job({ name: 'summary', version: 1 })
  readonly summary = this.job({ payload: z.array(z.number()), result: z.number() })
  @Job({ name: 'double', version: 1 })
  readonly double = this.job({ payload: z.number(), result: z.number() })
}
const parent = flowJob(FlowQueue, 'summary')
const child = flowJob(FlowQueue, 'double')
let captured: FlowResultsReader | undefined
@Worker({ name: 'flow-worker', concurrency: 1, pollIntervalMs: 5, flowSweepIntervalMs: 10 })
@Flow({ name: 'summary', parent, children: [child], onChildFailure: 'continue' })
class FlowWorker {
  @FanOut()
  split(@FlowData() values: number[]) {
    return [
      flowChildren(
        child,
        values.map((payload, i) => ({ key: `item-${i}`, payload }))
      )
    ]
  }
  @Collect()
  async collect(@FlowChildren() results: FlowResultsReader) {
    captured = results
    const values = await results.all(child, { maxItems: 100 })
    return values.reduce((sum, row) => (row.outcome === 'completed' ? sum + row.result : sum), 0)
  }
  @Process(FlowQueue, 'double')
  double(@JobData() value: number) {
    return value * 2
  }
}
async function appFor(fixture: ReturnType<typeof flowConnection>, workers = true) {
  return Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection }, execution: { workers } }),
      MqModule.forFeature([FlowQueue])
    ],
    providers: [FlowWorker]
  }).compile()
}
test('a real concurrency-one Worker executes fan-out and persisted collection without a dummy handler', async () => {
  const fixture = flowConnection()
  const app = await appFor(fixture)
  try {
    await app.init()
    const queue = app.get(FlowQueue)
    const id = await queue.summary.enqueue([1, 2, 3])
    expect(await queue.summary.awaitResult(id, { timeoutMs: 3_000, pollIntervalMs: 5 })).toBe(12)
    expect(await app.get(MqFlowsService).get(parent, id)).toMatchObject({
      name: 'summary',
      counts: { pending: 0, completed: 3 },
      children: 3
    })
    assert.ok(captured)
    await assert.rejects(captured.page(child), MqFlowException)
    const empty = await queue.summary.execute([], { wait: { timeoutMs: 3_000, pollIntervalMs: 5 } })
    expect(empty).toBe(0)
  } finally {
    await app.close()
  }
  expect(fixture.trace).toEqual({ acquired: 1, released: 1 })
})
test('declared flows reject a missing opt-in before acquiring scoped stores', async () => {
  const fixture = flowConnection(false)
  const app = await appFor(fixture)
  await assert.rejects(app.init(), MqFlowException)
  expect(fixture.trace).toEqual({ acquired: 0, released: 0 })
  await assert.rejects(app.close(), MqFlowException)
})
test('producer-only contexts do not start flow phases and allow scoped reads/cancel before fan-out', async () => {
  const fixture = flowConnection()
  const app = await appFor(fixture, false)
  try {
    await app.init()
    const id = await app.get(FlowQueue).summary.enqueue([1])
    const service = app.get(MqFlowsService)
    expect(await service.get(parent, id)).toBeUndefined()
    await assert.rejects(service.get(child, id), MqFlowException)
    await service.cancel(parent, id)
    expect((await app.get(FlowQueue).summary.poll(id))?.state).toBe('cancelled')
  } finally {
    await app.close()
  }
})
