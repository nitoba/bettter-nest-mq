import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  Job,
  JobContext,
  JobData,
  JobFailureException,
  Retry,
  RetryPolicy,
  MqModule,
  MqFlowsService,
  Process,
  Queue,
  QueueService,
  Worker,
  Flow,
  FanOut,
  Collect,
  FlowData,
  FlowChildren,
  flowJob,
  flowChildren,
  type FlowResultsReader,
  type JobExecutionContext
} from '../../src/index.ts'
import { flowConnection } from '../fixtures/flow-connection.ts'

@Queue({ name: 'retry-flows', connection: 'primary' })
class Tasks extends QueueService {
  @Job({ name: 'parent', version: 1 })
  readonly parent = this.job({
    payload: z.enum(['custom', 'attempts', 'override']),
    result: z.number()
  })
  @Job({ name: 'custom', version: 1 })
  @Retry({ attempts: 3, backoff: { type: 'custom', policy: 'child', version: 1 } })
  readonly custom = this.job({
    payload: z.number(),
    result: z.number(),
    failure: z.object({ busy: z.boolean() }),
    retryable: (failure) => failure.busy
  })
  @Job({ name: 'ordinary', version: 1 })
  @Retry({ attempts: 3, backoff: { type: 'fixed', delayMs: 5 } })
  readonly ordinary = this.job({ payload: z.number(), result: z.number() })
}
const parent = flowJob(Tasks, 'parent')
const custom = flowJob(Tasks, 'custom')
const ordinary = flowJob(Tasks, 'ordinary')
@RetryPolicy({ name: 'child', version: 1 })
class ChildRetry {
  decide() {
    return { retry: true, delayMs: 2 }
  }
}
@Worker({ name: 'retry-flows', concurrency: 1, pollIntervalMs: 5, flowSweepIntervalMs: 10 })
@Flow({ name: 'retry-flows', parent, children: [custom, ordinary], onChildFailure: 'continue' })
class Consumer {
  @FanOut() split(@FlowData() mode: 'custom' | 'attempts' | 'override') {
    return mode === 'custom'
      ? [flowChildren(custom, [{ key: 'a', payload: 3 }])]
      : [
          flowChildren(ordinary, [
            {
              key: 'a',
              payload: 3,
              options: {
                retry: {
                  attempts: 1,
                  backoff: { type: 'fixed', delayMs: mode === 'override' ? 9 : 5 }
                }
              }
            }
          ])
        ]
  }
  @Collect() async collect(@FlowChildren() results: FlowResultsReader) {
    const rows = [
      ...(await results.all(custom, { maxItems: 1 })),
      ...(await results.all(ordinary, { maxItems: 1 }))
    ]
    assert.equal(rows.length, 1)
    assert.ok(rows[0]?.outcome === 'completed')
    return rows[0].result
  }
  @Process(Tasks, 'custom') custom(
    @JobData() value: number,
    @JobContext() context: JobExecutionContext
  ) {
    if (context.attempt === 1) throw new JobFailureException({ busy: true })
    return value
  }
  @Process(Tasks, 'ordinary') ordinary(@JobData() value: number) {
    return value
  }
}
for (const mode of ['custom', 'attempts'] as const) {
  test(`flow ${mode} child plans preserve declared retry policies through actual native phases`, async () => {
    const fixture = flowConnection()
    const app = await Test.createTestingModule({
      imports: [
        MqModule.forRoot({ connections: { primary: fixture.connection } }),
        MqModule.forFeature([Tasks])
      ],
      providers: [Consumer, ChildRetry]
    }).compile()
    await app.init()
    try {
      expect(
        await app.get(Tasks).parent.execute(mode, { wait: { timeoutMs: 3_000, pollIntervalMs: 5 } })
      ).toBe(3)
    } finally {
      await app.close()
    }
  })
}

test('unsupported flow child backoff changes reject before writing a manifest', async () => {
  const fixture = flowConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature([Tasks])
    ],
    providers: [Consumer, ChildRetry]
  }).compile()
  await app.init()
  try {
    const task = app.get(Tasks).parent
    const id = await task.enqueue('override')
    await assert.rejects(task.awaitResult(id, { timeoutMs: 3_000, pollIntervalMs: 5 }))
    expect(await app.get(MqFlowsService).get(parent, id)).toBeUndefined()
    expect((await task.attempts(id))[0]?.failure?.kind).toBe('defect')
  } finally {
    await app.close()
  }
})
