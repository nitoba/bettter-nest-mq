import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Inject, Injectable, type Provider, type Type } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  ContractDefinitionException,
  Job,
  JobData,
  MqModule,
  Process,
  Queue,
  QueueService,
  Worker,
  UseMqGuards,
  UseMqPipes,
  UseMqFilters,
  type MqExecutionContext
} from '../../src/index.ts'
import { zodCodec } from '../../src/integrations/zod.ts'
import { memoryConnection } from '../fixtures/memory-connection.ts'

const wait = { timeoutMs: 3_000, pollIntervalMs: 5 }
@Queue({ name: 'boundaries', connection: 'primary' })
class Tasks extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({ payload: z.number(), result: z.number() })
}
@Injectable()
class Audit {
  readonly entries: string[] = []
}
async function application(providers: Provider[], queues: Type<QueueService>[] = [Tasks]) {
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature(queues)
    ],
    providers
  }).compile()
  return { app, fixture }
}

test('the nearest supporting MQ filter handles a failure and its result still obeys the output schema', async () => {
  class Never {
    constructor(@Inject(Audit) private readonly audit: Audit) {}
    supports() {
      this.audit.entries.push('method-skip')
      return false
    }
    catch() {
      throw new Error('unreachable')
    }
  }
  class Local {
    constructor(@Inject(Audit) private readonly audit: Audit) {}
    supports(cause: Error) {
      this.audit.entries.push('method-supports')
      return cause.message === 'local'
    }
    catch() {
      this.audit.entries.push('method-catch')
      return 7
    }
  }
  class Global {
    constructor(@Inject(Audit) private readonly audit: Audit) {}
    supports() {
      this.audit.entries.push('class-supports')
      return true
    }
    catch(cause: Error) {
      this.audit.entries.push('class-catch')
      return cause.message === 'invalid' ? NaN : 9
    }
  }
  @Worker({ name: 'filters', pollIntervalMs: 5 })
  @UseMqFilters(Global)
  class Consumer {
    @Process(Tasks, 'task')
    @UseMqFilters(Never, Local)
    run(@JobData() value: number) {
      throw new Error(value === 1 ? 'local' : value === 2 ? 'global' : 'invalid')
    }
  }
  const { app } = await application([Consumer, Audit, Never, Local, Global])
  await app.init()
  try {
    const task = app.get(Tasks).task
    expect(await task.execute(1, { wait })).toBe(7)
    expect(app.get(Audit).entries).toEqual(['method-skip', 'method-supports', 'method-catch'])
    app.get(Audit).entries.length = 0
    expect(await task.execute(2, { wait })).toBe(9)
    expect(app.get(Audit).entries).toEqual([
      'method-skip',
      'method-supports',
      'class-supports',
      'class-catch'
    ])
    const id = await task.enqueue(3)
    await assert.rejects(task.awaitResult(id, wait))
    expect((await task.attempts(id))[0]?.failure?.kind).toBe('encode')
  } finally {
    await app.close()
  }
})

test('pipes cannot pass a schema-invalid decoded value to business code', async () => {
  let calls = 0
  class Invalid {
    transform() {
      return NaN
    }
  }
  @Worker({ name: 'invalid-pipe', pollIntervalMs: 5 })
  @UseMqPipes(Invalid)
  class Consumer {
    @Process(Tasks, 'task') run(@JobData() value: number) {
      calls++
      return value
    }
  }
  const { app } = await application([Consumer, Invalid])
  await app.init()
  try {
    await assert.rejects(app.get(Tasks).task.execute(1, { wait }))
    expect(calls).toBe(0)
  } finally {
    await app.close()
  }
})

test('pipes work with decoded Date codecs rather than reapplying one-way input transformations', async () => {
  const date = z.codec(z.iso.datetime(), z.date(), {
    decode: (value) => new Date(value),
    encode: (value) => value.toISOString()
  })
  @Queue({ name: 'dates', connection: 'primary' })
  class Dates extends QueueService {
    @Job({ name: 'date', version: 1 })
    readonly date = this.job({ payload: zodCodec(date), result: zodCodec(date) })
  }
  class Tomorrow {
    transform(value: Date, context: MqExecutionContext<Date>) {
      assert.ok(value instanceof Date)
      assert.equal(context.payload, value)
      return new Date(value.getTime() + 86_400_000)
    }
  }
  @Worker({ name: 'dates', pollIntervalMs: 5 })
  class Consumer {
    @Process(Dates, 'date')
    @UseMqPipes(Tomorrow)
    run(@JobData() value: Date) {
      assert.ok(value instanceof Date)
      return value
    }
  }
  const { app } = await application([Consumer, Tomorrow], [Dates])
  await app.init()
  try {
    expect(await app.get(Dates).date.execute('2026-09-11T00:00:00.000Z', { wait })).toEqual(
      new Date('2026-09-12T00:00:00.000Z')
    )
  } finally {
    await app.close()
  }
})

test('class enhancers compose base-first while a method override does not inherit the replaced implementation metadata', async () => {
  class First {
    constructor(@Inject(Audit) private readonly audit: Audit) {}
    canActivate() {
      this.audit.entries.push('base')
      return true
    }
  }
  class Second {
    constructor(@Inject(Audit) private readonly audit: Audit) {}
    canActivate() {
      this.audit.entries.push('derived')
      return true
    }
  }
  class Increment {
    transform(value: number) {
      return value + 1
    }
  }
  @UseMqGuards(First)
  class Base {
    @Process(Tasks, 'task')
    @UseMqPipes(Increment)
    run(@JobData() value: number) {
      return value
    }
  }
  @Worker({ name: 'inherited', pollIntervalMs: 5 })
  @UseMqGuards(Second)
  class Inherited extends Base {}
  @Worker({ name: 'overridden', pollIntervalMs: 5 })
  @UseMqGuards(Second)
  class Overridden extends Base {
    @Process(Tasks, 'task') override run(@JobData() value: number) {
      return value
    }
  }
  for (const [consumer, result] of [
    [Inherited, 2],
    [Overridden, 1]
  ] as const) {
    const { app } = await application([consumer, First, Second, Increment, Audit])
    await app.init()
    try {
      expect(await app.get(Tasks).task.execute(1, { wait })).toBe(result)
      expect(app.get(Audit).entries).toEqual(['base', 'derived'])
    } finally {
      await app.close()
    }
  }
})

test('enhancer accessors are rejected at preflight without executing the getter', async () => {
  let reads = 0
  class Accessor {
    get canActivate() {
      reads++
      return () => true
    }
  }
  @Worker({ name: 'accessor', pollIntervalMs: 5 })
  @UseMqGuards(Accessor)
  class Consumer {
    @Process(Tasks, 'task') run(@JobData() value: number) {
      return value
    }
  }
  const { app, fixture } = await application([Consumer, Accessor])
  await assert.rejects(app.init(), ContractDefinitionException)
  expect(fixture.trace.acquisitions).toBe(0)
  expect(reads).toBe(0)
  await assert.rejects(app.close(), ContractDefinitionException)
})

test('cancellation during an asynchronous guard cannot be recovered by a filter into a handler execution', async () => {
  const entered = Promise.withResolvers<void>()
  let calls = 0
  let filtered = 0
  class Held {
    async canActivate(context: MqExecutionContext) {
      entered.resolve()
      if (!context.job.signal.aborted)
        await new Promise<void>((resolve) =>
          context.job.signal.addEventListener('abort', () => resolve(), { once: true })
        )
      return true
    }
  }
  class Recover {
    supports() {
      filtered++
      return true
    }
    catch() {
      return 99
    }
  }
  @Worker({
    name: 'cancelled-guard',
    pollIntervalMs: 5,
    leaseDurationMs: 300,
    heartbeatIntervalMs: 20
  })
  @UseMqGuards(Held)
  @UseMqFilters(Recover)
  class Consumer {
    @Process(Tasks, 'task') run(@JobData() value: number) {
      calls++
      return value
    }
  }
  const { app } = await application([Consumer, Held, Recover])
  await app.init()
  try {
    const task = app.get(Tasks).task
    const id = await task.enqueue(1)
    await entered.promise
    await task.cancel(id)
    await assert.rejects(task.awaitResult(id, wait))
    expect((await task.poll(id))?.state).toBe('cancelled')
    expect(calls).toBe(0)
    expect(filtered).toBe(0)
  } finally {
    await app.close()
  }
})
