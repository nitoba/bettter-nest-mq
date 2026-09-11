import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Injectable, Scope } from '@nestjs/common'
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
  MqFlowException,
  Queue,
  QueueService,
  Process,
  Worker,
  type FlowResultsReader
} from '../../src/index.ts'
import { flowConnection } from '../fixtures/flow-connection.ts'
import { zodCodec } from '../../src/integrations/zod.ts'
import { Result } from 'better-result'

const date = z.codec(z.iso.datetime(), z.date(), {
  decode: (text) => new Date(text),
  encode: (value) => value.toISOString()
})
@Queue({ name: 'boundary', connection: 'primary' })
class BoundaryQueue extends QueueService {
  @Job({ name: 'parent', version: 1 })
  readonly parent = this.job({ payload: z.string(), result: z.string() })
  @Job({ name: 'child', version: 1 })
  readonly child = this.job({
    payload: zodCodec(date),
    result: zodCodec(date),
    failure: z.object({ reason: z.string() })
  })
  @Job({ name: 'keyed', version: 1 })
  readonly keyed = this.job({
    payload: z.string(),
    result: z.string(),
    idempotencyKey: (value) => value
  })
}
const parent = flowJob(BoundaryQueue, 'parent')
const child = flowJob(BoundaryQueue, 'child')
const keyed = flowJob(BoundaryQueue, 'keyed')
const scopes: number[] = []
let nextScope = 0
@Injectable({ scope: Scope.REQUEST })
class AttemptDependency {
  readonly id = ++nextScope
}
@Worker({ name: 'boundaries', concurrency: 1, pollIntervalMs: 5, flowSweepIntervalMs: 10 })
@Flow({
  name: 'boundary',
  parent,
  children: [child, keyed],
  onChildFailure: 'continue',
  maxChildren: 10
})
class BoundaryWorker {
  constructor(private readonly dependency: AttemptDependency) {}
  @FanOut() split(@FlowData() mode: string) {
    scopes.push(this.dependency.id)
    if (mode === 'global-key')
      return [
        flowChildren(keyed, [
          { key: 'a', payload: 'same' },
          { key: 'b', payload: 'same' }
        ])
      ]
    if (mode === 'invalid') return [flowChildren(child, [{ key: 'bad', payload: 'not-a-date' }])]
    return [flowChildren(child, [{ key: 'date', payload: '2026-09-11T12:00:00.000Z' }])]
  }
  @Collect() async finish(@FlowData() mode: string, @FlowChildren() reader: FlowResultsReader) {
    scopes.push(this.dependency.id)
    if (mode === 'overflow') return String((await reader.all(child, { maxItems: 0 })).length)
    const result = await reader.all(child, { maxItems: 10 })
    assert.ok(result[0]?.outcome === 'completed')
    return result[0].result.toISOString()
  }
  @Process(BoundaryQueue, 'child') child(@JobData() value: Date) {
    assert.ok(value instanceof Date)
    return value
  }
  @Process(BoundaryQueue, 'keyed') keyed(@JobData() value: string) {
    return value
  }
}
async function setup() {
  const fixture = flowConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature([BoundaryQueue])
    ],
    providers: [BoundaryWorker, AttemptDependency]
  }).compile()
  await app.init()
  return { app, fixture }
}
test('flow phases resolve actual request-scoped Nest dependencies and preserve decoded Date results', async () => {
  scopes.length = 0
  const { app } = await setup()
  try {
    const result = await app
      .get(BoundaryQueue)
      .parent.execute('date', { wait: { timeoutMs: 2_000, pollIntervalMs: 5 } })
    expect(result).toBe('2026-09-11T12:00:00.000Z')
    expect(scopes).toHaveLength(2)
    expect(new Set(scopes).size).toBe(2)
  } finally {
    await app.close()
  }
})
test.each(['invalid', 'global-key'])(
  'invalid fan-out %s fails before persisting any child manifest',
  async (mode) => {
    const { app, fixture } = await setup()
    try {
      const id = await app.get(BoundaryQueue).parent.enqueue(mode)
      await assert.rejects(
        app.get(BoundaryQueue).parent.awaitResult(id, { timeoutMs: 2_000, pollIntervalMs: 5 })
      )
      const count = await fixture.jobs.counts()
      if (Result.isError(count)) throw count.error
      expect(count.value.total).toBe(1)
    } finally {
      await app.close()
    }
  }
)
test('bounded collection rejects overflow instead of silently discarding results', async () => {
  const { app } = await setup()
  try {
    await assert.rejects(
      app
        .get(BoundaryQueue)
        .parent.execute('overflow', { wait: { timeoutMs: 2_000, pollIntervalMs: 5 } })
    )
  } finally {
    await app.close()
  }
})
test('a flow-only Worker is rejected before resource acquisition rather than supplied a fabricated handler', async () => {
  @Worker({ name: 'flow-only' })
  @Flow({ name: 'flow-only', parent, children: [], onChildFailure: 'continue' })
  class FlowOnly {
    @FanOut() split() {
      return []
    }
    @Collect() collect() {
      return 'done'
    }
  }
  const fixture = flowConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature([BoundaryQueue])
    ],
    providers: [FlowOnly]
  }).compile()
  await assert.rejects(app.init(), MqFlowException)
  expect(fixture.trace.acquired).toBe(0)
  await assert.rejects(app.close(), MqFlowException)
})
