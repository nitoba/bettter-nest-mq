import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Test } from '@nestjs/testing'
import { Result } from 'better-result'
import type { TickScheduleCommand } from 'better-effect-mq'
import { z } from 'zod'
import {
  Job,
  MqModule,
  MqScheduleException,
  MqSchedulesService,
  Queue,
  QueueService,
  Schedule
} from '../../src/index.ts'
import { scheduleConnection } from '../fixtures/schedule-connection.ts'

@Queue({ name: 'lifecycle', connection: 'primary' })
class LifecycleQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  @Schedule({ key: 'regular', everyMs: 60_000, payload: { value: 'original' } })
  readonly task = this.job({ payload: z.object({ value: z.string() }), result: z.string() })
}
async function appFor(
  fixture: ReturnType<typeof scheduleConnection>,
  mode: 'validate' | 'reconcile',
  scheduler = false
) {
  return Test.createTestingModule({
    imports: [
      MqModule.forRoot({
        connections: { primary: fixture.connection },
        schedules: { mode, sweepIntervalMs: 60_000 },
        execution: { workers: false, scheduler, outboxPublisher: false }
      }),
      MqModule.forFeature([LifecycleQueue])
    ]
  }).compile()
}

test('changed payload drift fails validation without rewriting the operator definition', async () => {
  const fixture = scheduleConnection()
  const deploy = await appFor(fixture, 'reconcile')
  try {
    await deploy.init()
    await deploy.get(MqSchedulesService).upsert(LifecycleQueue, 'task', {
      key: 'regular',
      everyMs: 60_000,
      payload: { value: 'operator' }
    })
  } finally {
    await deploy.close()
  }
  const replica = await appFor(fixture, 'validate')
  await assert.rejects(replica.init(), (error) => {
    assert.ok(error instanceof MqScheduleException)
    expect(error.phase).toBe('drift')
    return true
  })
  await assert.rejects(replica.close(), MqScheduleException)
  const record = await fixture.schedules.getSchedule({ group: 'nestjs/schedules', key: 'regular' })
  if (Result.isError(record)) throw record.error
  expect(record.value?.payload).toEqual({ value: 'operator' })
})

test('all static payloads validate before any schedule or store is acquired', async () => {
  @Queue({ name: 'bad-payload', connection: 'primary' })
  class InvalidQueue extends QueueService {
    @Job({ name: 'first', version: 1 })
    @Schedule({ key: 'good', everyMs: 10, payload: { value: 'valid' } })
    readonly first = this.job({ payload: z.object({ value: z.string() }), result: z.string() })
    @Job({ name: 'second', version: 1 })
    @Schedule({ key: 'invalid', everyMs: 10, payload: { value: 1 } })
    readonly second = this.job({ payload: z.object({ value: z.string() }), result: z.string() })
  }
  const fixture = scheduleConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({
        connections: { primary: fixture.connection },
        schedules: { mode: 'reconcile' }
      }),
      MqModule.forFeature([InvalidQueue])
    ]
  }).compile()
  await assert.rejects(app.init())
  expect(fixture.trace).toEqual({ acquired: 0, released: 0 })
  const records = await fixture.schedules.listSchedules()
  if (Result.isError(records)) throw records.error
  expect(records.value).toEqual([])
  await assert.rejects(app.close())
})

test('closing a scheduler waits for its admitted upstream tick before releasing the store', async () => {
  const fixture = scheduleConnection()
  const deploy = await appFor(fixture, 'reconcile')
  await deploy.init()
  await deploy.close()
  const record = await fixture.schedules.getSchedule({ group: 'nestjs/schedules', key: 'regular' })
  if (Result.isError(record)) throw record.error
  assert.ok(record.value)
  const removed = await fixture.schedules.removeSchedule({
    group: 'nestjs/schedules',
    key: 'regular'
  })
  if (Result.isError(removed)) throw removed.error
  const seeded = await fixture.schedules.upsertSchedule({
    ...record.value,
    nextRunAtMs: Date.now() - 1
  })
  if (Result.isError(seeded)) throw seeded.error
  const entered = Promise.withResolvers<void>()
  const gate = Promise.withResolvers<void>()
  const original = fixture.schedules.tickSchedule.bind(fixture.schedules)
  Object.defineProperty(fixture.schedules, 'tickSchedule', {
    value: async (command: TickScheduleCommand) => {
      entered.resolve()
      await gate.promise
      return original(command)
    }
  })
  const worker = await appFor(fixture, 'validate', true)
  try {
    await worker.init()
    await entered.promise
    const stopping = worker.close()
    expect(fixture.trace.released).toBe(1)
    gate.resolve()
    await stopping
    expect(fixture.trace.released).toBe(2)
    expect(worker.get(MqSchedulesService).scheduler()).toBeUndefined()
    const jobs = await fixture.jobs.counts()
    if (Result.isError(jobs)) throw jobs.error
    expect(jobs.value.total).toBe(1)
  } finally {
    gate.resolve()
    await worker.close()
  }
})
