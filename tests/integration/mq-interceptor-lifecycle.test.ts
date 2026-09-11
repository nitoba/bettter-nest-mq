import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { setTimeout as delay } from 'node:timers/promises'
import type { Provider } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  Job,
  JobData,
  MqModule,
  Process,
  Queue,
  QueueService,
  Worker,
  UseMqInterceptors,
  type MqExecutionContext,
  type MqNext
} from '../../src/index.ts'
import { memoryConnection } from '../fixtures/memory-connection.ts'

const wait = { timeoutMs: 3_000, pollIntervalMs: 5 }
@Queue({ name: 'interceptors', connection: 'primary' })
class Tasks extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({ payload: z.number(), result: z.number() })
}
async function application(providers: Provider[]) {
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature([Tasks])
    ],
    providers
  }).compile()
  await app.init()
  return { app, fixture, task: app.get(Tasks).task }
}

test('interceptor continuations cannot dispatch the same handler twice even when the caller catches the rejection', async () => {
  let calls = 0
  class Twice {
    async intercept(_context: MqExecutionContext, next: MqNext) {
      await next()
      await next().catch(() => undefined)
      return 42
    }
  }
  @Worker({ name: 'twice', pollIntervalMs: 5 })
  @UseMqInterceptors(Twice)
  class Consumer {
    @Process(Tasks, 'task') run(@JobData() value: number) {
      calls++
      return value
    }
  }
  const { app, task } = await application([Consumer, Twice])
  try {
    await assert.rejects(task.execute(1, { wait }))
    expect(calls).toBe(1)
  } finally {
    await app.close()
  }
})

test('an escaped next cannot execute business code after its attempt has settled', async () => {
  let escaped: MqNext | undefined
  let calls = 0
  class Escape {
    intercept(_context: MqExecutionContext, next: MqNext) {
      escaped = next
      return 42
    }
  }
  @Worker({ name: 'escape', pollIntervalMs: 5 })
  @UseMqInterceptors(Escape)
  class Consumer {
    @Process(Tasks, 'task') run(@JobData() value: number) {
      calls++
      return value
    }
  }
  const { app, task } = await application([Consumer, Escape])
  try {
    expect(await task.execute(1, { wait })).toBe(42)
    assert.ok(escaped)
    await assert.rejects(escaped())
    expect(calls).toBe(0)
  } finally {
    await app.close()
  }
})

test('an admitted next drains before settlement and before application resources close', async () => {
  const entered = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  let finished = false
  class EarlyReturn {
    intercept(_context: MqExecutionContext, next: MqNext) {
      void next().catch(() => undefined)
      return 42
    }
  }
  @Worker({ name: 'drain', pollIntervalMs: 5 })
  @UseMqInterceptors(EarlyReturn)
  class Consumer {
    @Process(Tasks, 'task') async run(@JobData() value: number) {
      entered.resolve()
      await release.promise
      finished = true
      return value
    }
  }
  const { app, task, fixture } = await application([Consumer, EarlyReturn])
  try {
    const id = await task.enqueue(1)
    await entered.promise
    await delay(20)
    expect((await task.poll(id))?.state).toBe('active')
    const closing = app.close()
    await delay(20)
    expect(fixture.trace.resourceReleases).toBe(0)
    release.resolve()
    await closing
    expect(finished).toBe(true)
    expect(fixture.trace.resourceReleases).toBe(1)
  } finally {
    release.resolve()
    await app.close()
  }
})
