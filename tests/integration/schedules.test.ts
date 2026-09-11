import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Test } from '@nestjs/testing'
import { Result } from 'better-result'
import { z } from 'zod'
import {
  Job,
  JobData,
  MqModule,
  MqSchedulesService,
  MqScheduleException,
  Process,
  Queue,
  QueueService,
  Schedule,
  Worker,
  type MqModuleOptions
} from '../../src/index.ts'
import { scheduleConnection } from '../fixtures/schedule-connection.ts'

@Queue({ name: 'schedules', connection: 'primary' })
class SchedulesQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  @Schedule({
    key: 'regular',
    everyMs: 60_000,
    payload: { text: 'scheduled' },
    misfire: { strategy: 'run-once' }
  })
  readonly task = this.job({ payload: z.object({ text: z.string() }), result: z.string() })
  @Job({ name: 'other', version: 1 })
  readonly other = this.job({ payload: z.string(), result: z.string() })
}
@Worker({ name: 'schedule-test', pollIntervalMs: 5 })
class SchedulesWorker {
  @Process(SchedulesQueue, 'task')
  task(@JobData() payload: { text: string }) {
    return payload.text.toUpperCase()
  }
}
async function application(
  fixture: ReturnType<typeof scheduleConnection>,
  overrides: Partial<MqModuleOptions> = {}
) {
  return Test.createTestingModule({
    imports: [
      MqModule.forRoot({
        connections: { primary: fixture.connection },
        schedules: { mode: 'reconcile', sweepIntervalMs: 60_000 },
        execution: { scheduler: false },
        ...overrides
      }),
      MqModule.forFeature([SchedulesQueue])
    ],
    providers: [SchedulesWorker]
  }).compile()
}

test('missing opt-in schedule resource fails before scoped store acquisition', async () => {
  const fixture = scheduleConnection(false)
  const app = await application(fixture)
  await assert.rejects(app.init(), MqScheduleException)
  expect(fixture.trace).toEqual({ acquired: 0, released: 0 })
  await assert.rejects(app.close(), MqScheduleException)
})
test('ordinary replicas do not create a missing declaration', async () => {
  const fixture = scheduleConnection()
  const app = await application(fixture, { schedules: { mode: 'validate' } })
  await assert.rejects(app.init(), MqScheduleException)
  const records = await fixture.schedules.listSchedules()
  if (Result.isError(records)) throw records.error
  expect(records.value).toEqual([])
  await assert.rejects(app.close(), MqScheduleException)
})
test('reconciliation preserves pause, revision and next occurrence without disabling omitted schedules', async () => {
  const fixture = scheduleConnection()
  const app = await application(fixture)
  let revision: number
  let nextRunAtMs: number
  try {
    await app.init()
    const service = app.get(MqSchedulesService)
    const record = await service.get(SchedulesQueue, 'task', 'regular')
    assert.ok(record)
    nextRunAtMs = record.nextRunAtMs
    await service.pause(SchedulesQueue, 'task', 'regular')
    const paused = await service.get(SchedulesQueue, 'task', 'regular')
    assert.ok(paused)
    revision = paused.revision
    await service.upsert(SchedulesQueue, 'other', {
      key: 'dynamic',
      everyMs: 90_000,
      payload: 'keep'
    })
    await service.reconcile()
    expect(await service.get(SchedulesQueue, 'task', 'regular')).toMatchObject({
      paused: true,
      revision,
      nextRunAtMs
    })
    expect(await service.get(SchedulesQueue, 'other', 'dynamic')).toBeDefined()
  } finally {
    await app.close()
  }
  const replica = await application(fixture, { schedules: { mode: 'validate' } })
  try {
    await replica.init()
    const service = replica.get(MqSchedulesService)
    expect(await service.get(SchedulesQueue, 'task', 'regular')).toMatchObject({
      paused: true,
      revision,
      nextRunAtMs
    })
    await assert.rejects(service.reconcile(), MqScheduleException)
  } finally {
    await replica.close()
  }
})
test('admin operations are contract-scoped, schema-validated and do not reassign an existing address', async () => {
  const fixture = scheduleConnection()
  const app = await application(fixture)
  try {
    await app.init()
    const service = app.get(MqSchedulesService)
    await assert.rejects(
      service.upsert(SchedulesQueue, 'task', {
        key: 'invalid',
        everyMs: 10,
        payload: JSON.parse('{"text":1}')
      }),
      Error
    )
    expect(await service.get(SchedulesQueue, 'task', 'invalid')).toBeUndefined()
    await assert.rejects(service.get(SchedulesQueue, 'other', 'regular'), MqScheduleException)
    await assert.rejects(
      service.upsert(SchedulesQueue, 'other', {
        key: 'regular',
        everyMs: 10,
        payload: 'wrong owner'
      }),
      MqScheduleException
    )
    expect(await service.list(SchedulesQueue, 'other')).toEqual([])
    await service.resume(SchedulesQueue, 'task', 'regular')
    expect(await service.remove(SchedulesQueue, 'task', 'regular')).toBe(true)
    expect(await service.remove(SchedulesQueue, 'task', 'regular')).toBe(false)
  } finally {
    await app.close()
  }
  await assert.rejects(app.get(MqSchedulesService).get(SchedulesQueue, 'task', 'regular'), Error)
})
test('upstream sweeps enqueue real jobs, and a second sweep cannot fire the same occurrence', async () => {
  const fixture = scheduleConnection()
  const app = await application(fixture, {
    execution: { scheduler: true },
    schedules: { mode: 'reconcile', sweepIntervalMs: 60_000 }
  })
  try {
    await app.init()
    const service = app.get(MqSchedulesService)
    const record = await fixture.schedules.getSchedule({
      group: 'nestjs/schedules',
      key: 'regular'
    })
    if (Result.isError(record)) throw record.error
    assert.ok(record.value)
    // An unchanged upsert intentionally preserves the cursor. Seed a fresh overdue record
    // in this isolated reference fixture instead of expecting upsert to reset runtime state.
    const removed = await fixture.schedules.removeSchedule({
      group: 'nestjs/schedules',
      key: 'regular'
    })
    if (Result.isError(removed)) throw removed.error
    const set = await fixture.schedules.upsertSchedule({
      ...record.value,
      nextRunAtMs: Date.now() - 1
    })
    if (Result.isError(set)) throw set.error
    await service.sweep()
    const fired = await service.get(SchedulesQueue, 'task', 'regular')
    assert.ok(fired?.lastJobId)
    expect(
      await app
        .get(SchedulesQueue)
        .task.awaitResult(fired.lastJobId, { timeoutMs: 2_000, pollIntervalMs: 5 })
    ).toBe('SCHEDULED')
    await service.sweep()
    const counts = await fixture.jobs.counts()
    if (Result.isError(counts)) throw counts.error
    expect(counts.value.total).toBe(1)
    expect(service.scheduler()).toMatchObject({ state: 'running', activeTickCount: 0 })
  } finally {
    await app.close()
  }
  expect(app.get(MqSchedulesService).scheduler()).toBeUndefined()
})
