import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import { ContractDefinitionException, Job, JobData, MqModule, Process, Queue, QueueService, Worker } from '../../src/index.ts'
import { memoryConnection } from '../fixtures/memory-connection.ts'

class Tasks extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: z.string(), result: z.string() })
}
@Queue({ name: 'same-identity', connection: 'left' })
class Left extends Tasks {}
@Queue({ name: 'same-identity', connection: 'right' })
class Right extends Tasks {}
@Worker({ name: 'ambiguous-supervisor' })
class Ambiguous {
  @Process(Left, 'echo')
  left(@JobData() value: string) { return value }
  @Process(Right, 'echo')
  right(@JobData() value: string) { return value }
}

test('an unsupported cross-connection identity collision fails before any store is acquired', async () => {
  const left = memoryConnection()
  const right = memoryConnection()
  const app = await Test.createTestingModule({ imports: [MqModule.forRoot({ connections: { left: left.connection, right: right.connection } }), MqModule.forFeature([Left, Right])], providers: [Ambiguous] }).compile()
  await assert.rejects(app.init(), ContractDefinitionException)
  expect(left.trace.acquisitions).toBe(0)
  expect(right.trace.acquisitions).toBe(0)
  await assert.rejects(app.close(), ContractDefinitionException)
})

@Worker({ name: 'left-only', pollIntervalMs: 5 })
class LeftWorker {
  @Process(Left, 'echo')
  run(@JobData() value: string) { return `left:${value}` }
}
@Worker({ name: 'right-only', pollIntervalMs: 5 })
class RightWorker {
  @Process(Right, 'echo')
  run(@JobData() value: string) { return `right:${value}` }
}

test('separate worker Services preserve identical job names in independent connections', async () => {
  const left = memoryConnection()
  const right = memoryConnection()
  const app = await Test.createTestingModule({ imports: [MqModule.forRoot({ connections: { left: left.connection, right: right.connection } }), MqModule.forFeature([Left, Right])], providers: [LeftWorker, RightWorker] }).compile()
  try {
    await app.init()
    const options = { wait: { timeoutMs: 2_000, pollIntervalMs: 5 } }
    expect(await app.get(Left).echo.execute('value', options)).toBe('left:value')
    expect(await app.get(Right).echo.execute('value', options)).toBe('right:value')
  } finally { await app.close() }
})
