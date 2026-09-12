import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Inject, Injectable, Scope } from '@nestjs/common'
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
  UseMqGuards,
  UseMqPipes,
  UseMqInterceptors,
  UseMqFilters,
  type MqExecutionContext,
  type MqNext,
  type JobExecutionContext
} from '../../src/index.ts'
import { memoryConnection } from '../fixtures/memory-connection.ts'

const wait = { timeoutMs: 3_000, pollIntervalMs: 5 }
@Queue({ name: 'enhancers', connection: 'primary' })
class Tasks extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({ payload: z.number(), result: z.number() })
}
@Injectable()
class Audit {
  readonly entries: string[] = []
  readonly scopes: object[] = []
}
@Injectable({ scope: Scope.REQUEST })
class Attempt {
  readonly events: string[] = []
}
@Injectable()
class Guard {
  constructor(
    @Inject(Attempt) private readonly attempt: Attempt,
    @Inject(Audit) private readonly audit: Audit
  ) {}
  canActivate(context: MqExecutionContext<number>) {
    assert.equal(context.type, 'mq')
    assert.equal(Object.isFrozen(context), true)
    assert.equal(context.phase, 'process')
    this.attempt.events.push('guard')
    this.audit.scopes.push(this.attempt)
    return context.payload >= 0
  }
}
@Injectable()
class Increment {
  constructor(@Inject(Attempt) private readonly attempt: Attempt) {}
  transform(value: number) {
    this.attempt.events.push('pipe')
    return value + 1
  }
}
@Injectable()
class Outer {
  constructor(@Inject(Attempt) private readonly attempt: Attempt) {}
  async intercept(_context: MqExecutionContext, next: MqNext<number>) {
    this.attempt.events.push('outer-before')
    const result = await next()
    this.attempt.events.push('outer-after')
    return result + 10
  }
}
@Injectable()
class Inner {
  constructor(@Inject(Attempt) private readonly attempt: Attempt) {}
  async intercept(context: MqExecutionContext, next: MqNext<number>) {
    assert.equal(context.payload, 2)
    this.attempt.events.push('inner-before')
    const result = await next()
    this.attempt.events.push('inner-after')
    return result * 2
  }
}
@Injectable()
class Capture {
  constructor(@Inject(Audit) private readonly audit: Audit) {}
  supports(cause: Error) {
    return cause.message === 'recover'
  }
  catch() {
    this.audit.entries.push('filter')
    return 23
  }
}
@Worker({ name: 'enhanced', pollIntervalMs: 5 })
@UseMqGuards(Guard)
@UseMqPipes(Increment)
@UseMqInterceptors(Outer)
class Consumer {
  constructor(
    @Inject(Attempt) private readonly attempt: Attempt,
    @Inject(Audit) private readonly audit: Audit
  ) {}
  @Process(Tasks, 'task')
  @UseMqInterceptors(Inner)
  @UseMqFilters(Capture)
  run(@JobData() value: number, @JobContext() context: JobExecutionContext) {
    this.attempt.events.push('handler')
    this.audit.scopes.push(this.attempt)
    this.audit.entries.push(context.jobId)
    return value * 3
  }
}

test('MQ class/method stages share the real worker DI scope and compose in declared order', async () => {
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature([Tasks])
    ],
    providers: [Consumer, Guard, Increment, Outer, Inner, Capture, Audit, Attempt]
  }).compile()
  await app.init()
  try {
    const task = app.get(Tasks).task
    expect(await task.execute(1, { wait })).toBe(22)
    const audit = app.get(Audit)
    expect(audit.scopes.length).toBe(2)
    expect(audit.scopes[0]).toBe(audit.scopes[1])
    assert.ok(audit.scopes[0] instanceof Attempt)
    expect(audit.scopes[0].events).toEqual([
      'guard',
      'pipe',
      'outer-before',
      'inner-before',
      'handler',
      'inner-after',
      'outer-after'
    ])
    expect(await task.execute(1, { wait })).toBe(22)
    expect(audit.scopes[2]).not.toBe(audit.scopes[0])
    expect(audit.scopes[2]).toBe(audit.scopes[3])
    await assert.rejects(task.execute(-1, { wait }))
    expect(audit.entries.length).toBe(2)
  } finally {
    await app.close()
  }
})

test('unregistered MQ enhancer classes fail before storage is acquired', async () => {
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature([Tasks])
    ],
    providers: [Consumer, Audit, Attempt]
  }).compile()
  await assert.rejects(app.init(), ContractDefinitionException)
  expect(fixture.trace.acquisitions).toBe(0)
  await assert.rejects(app.close(), ContractDefinitionException)
})
