import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Inject, Injectable, Scope, UsePipes } from '@nestjs/common'
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
  Worker,
  type JobExecutionContext
} from '../../src/index.ts'
import { memoryConnection } from '../fixtures/memory-connection.ts'

@Queue({ name: 'scope', connection: 'primary' })
class ScopedQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({ payload: z.string(), result: z.string() })
}

@Injectable({ scope: Scope.REQUEST })
class RequestDependency {
  readonly id = crypto.randomUUID()
}

@Injectable({ scope: Scope.REQUEST })
@Worker({ name: 'scoped-worker', concurrency: 2, pollIntervalMs: 5 })
class ScopedWorker {
  constructor(@Inject(RequestDependency) private readonly dependency: RequestDependency) {}
  @Process(ScopedQueue, 'task')
  run(@JobContext() context: JobExecutionContext) {
    return `${this.dependency.id}:${context.jobId}`
  }
}

test('request-scoped handler dependencies use a fresh real Nest DI subtree per attempt', async () => {
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature([ScopedQueue])
    ],
    providers: [ScopedWorker, RequestDependency]
  }).compile()
  try {
    await app.init()
    const queue = app.get(ScopedQueue)
    const [first, second] = await queue.task.enqueueMany([
      { payload: 'first' },
      { payload: 'second' }
    ])
    assert.ok(first && second)
    const results = await Promise.all(
      [first, second].map((id) =>
        queue.task.awaitResult(id, { timeoutMs: 3_000, pollIntervalMs: 5 })
      )
    )
    expect(results[0]?.split(':')[0]).not.toBe(results[1]?.split(':')[0])
  } finally {
    await app.close()
  }
})

test('duplicate processors fail before any store is acquired', async () => {
  @Worker({ name: 'duplicate-one' })
  class One {
    @Process(ScopedQueue, 'task') run(@JobData() value: string) {
      return value
    }
  }
  @Worker({ name: 'duplicate-two' })
  class Two {
    @Process(ScopedQueue, 'task') run(@JobData() value: string) {
      return value
    }
  }
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature([ScopedQueue])
    ],
    providers: [One, Two]
  }).compile()
  await assert.rejects(app.init(), ContractDefinitionException)
  expect(fixture.trace.acquisitions).toBe(0)
  await assert.rejects(app.close(), ContractDefinitionException)
})

test('HTTP enhancer metadata is rejected rather than silently ignored by an MQ invocation', async () => {
  @Worker({ name: 'unsupported-http' })
  class HttpWorker {
    @UsePipes({
      transform(value: string) {
        return value
      }
    })
    @Process(ScopedQueue, 'task')
    run(@JobData() value: string) {
      return value
    }
  }
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature([ScopedQueue])
    ],
    providers: [HttpWorker]
  }).compile()
  await assert.rejects(app.init(), ContractDefinitionException)
  expect(fixture.trace.acquisitions).toBe(0)
  await assert.rejects(app.close(), ContractDefinitionException)
})

test('unannotated handler parameters are rejected before acquisition', async () => {
  @Worker({ name: 'missing-argument-metadata' })
  class Invalid {
    @Process(ScopedQueue, 'task') run(value: string) {
      return value
    }
  }
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature([ScopedQueue])
    ],
    providers: [Invalid]
  }).compile()
  await assert.rejects(app.init(), ContractDefinitionException)
  expect(fixture.trace.acquisitions).toBe(0)
  await assert.rejects(app.close(), ContractDefinitionException)
})
