import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  ContractDefinitionException,
  Job,
  JobContext,
  JobData,
  MqModule,
  Process,
  Queue,
  QueueService,
  Retry,
  Worker,
  type JobExecutionContext
} from '../../src/index.ts'
import { memoryConnection } from '../fixtures/memory-connection.ts'

@Queue({ name: 'inherited-workers', connection: 'primary' })
class InheritedQueue extends QueueService {
  @Job({ name: 'first', version: 1 })
  readonly first = this.job({ payload: z.string(), result: z.string() })
  @Job({ name: 'second', version: 1 })
  readonly second = this.job({ payload: z.string(), result: z.string() })
}

class BaseWorker {
  @Process(InheritedQueue, 'first')
  first(@JobData() value: string) {
    return `base:${value}`
  }
}

@Worker({ name: 'inherited-worker', pollIntervalMs: 5 })
class DerivedWorker extends BaseWorker {
  @Process(InheritedQueue, 'second')
  second(@JobContext() context: JobExecutionContext) {
    return context.name
  }
}

test('inherited processors remain registered when a subclass adds another decorated method', async () => {
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature([InheritedQueue])
    ],
    providers: [DerivedWorker]
  }).compile()
  try {
    await app.init()
    const queue = app.get(InheritedQueue)
    expect(
      await queue.first.execute('value', { wait: { timeoutMs: 500, pollIntervalMs: 5 } })
    ).toBe('base:value')
    expect(
      await queue.second.execute('value', { wait: { timeoutMs: 500, pollIntervalMs: 5 } })
    ).toBe('second')
  } finally {
    await app.close()
  }
})

test('one descriptor cannot silently acquire two different producer identities', async () => {
  @Queue({ name: 'aliased-descriptor', connection: 'primary' })
  class AliasedQueue extends QueueService {
    @Job({ name: 'first', version: 1 })
    readonly first = this.job({ payload: z.string(), result: z.string() })
    @Job({ name: 'second', version: 1 })
    readonly second = this.first
  }
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature([AliasedQueue])
    ]
  }).compile()
  try {
    await assert.rejects(app.init(), ContractDefinitionException)
  } finally {
    await app.close().catch(() => undefined)
  }
  expect(fixture.trace.acquisitions).toBe(0)
})

test('defect retries can be explicitly enabled without converting defects into typed failures', async () => {
  @Queue({ name: 'defect-opt-in', connection: 'primary' })
  class DefectQueue extends QueueService {
    @Job({ name: 'task', version: 1 })
    @Retry({ attempts: 2, backoff: { type: 'fixed', delayMs: 5 } })
    readonly task = this.job({ payload: z.string(), result: z.string() })
  }
  @Worker({ name: 'defect-opt-in', retryDefects: true, pollIntervalMs: 5 })
  class DefectWorker {
    @Process(DefectQueue, 'task')
    run(@JobData() value: string, @JobContext() context: JobExecutionContext) {
      if (context.attempt === 1) throw new Error('transient unexpected defect')
      return value
    }
  }
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature([DefectQueue])
    ],
    providers: [DefectWorker]
  }).compile()
  try {
    await app.init()
    const job = app.get(DefectQueue).task
    const id = await job.enqueue('success')
    expect(await job.awaitResult(id, { timeoutMs: 1_000, pollIntervalMs: 5 })).toBe('success')
    expect((await job.attempts(id))[0]?.failure?.kind).toBe('defect')
  } finally {
    await app.close()
  }
})
