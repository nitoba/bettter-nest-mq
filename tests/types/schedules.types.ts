import { z } from 'zod'
import {
  Job,
  QueueService,
  type MqSchedulesService,
  type ScheduleOptions
} from '../../src/index.ts'

class TypedQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({ payload: z.object({ count: z.number() }), result: z.string() })
  readonly notAJob = true
}
export const interval: ScheduleOptions<{ count: number }> = {
  key: 'valid',
  everyMs: 1_000,
  payload: { count: 1 }
}
// @ts-expect-error A schedule has exactly one cadence, never both.
export const mixed: ScheduleOptions = {
  key: 'bad',
  cron: '* * * * *',
  everyMs: 1_000,
  payload: null
}
export async function check(service: MqSchedulesService): Promise<void> {
  await service.upsert(TypedQueue, 'task', interval)
  // @ts-expect-error Dynamic schedule payloads retain the referenced job's input type.
  await service.upsert(TypedQueue, 'task', { key: 'bad', everyMs: 10, payload: { count: 'wrong' } })
  // @ts-expect-error Only real job properties are valid references.
  await service.get(TypedQueue, 'notAJob', 'key')
  // @ts-expect-error Missing job property is not a valid reference.
  await service.get(TypedQueue, 'missing', 'key')
}
