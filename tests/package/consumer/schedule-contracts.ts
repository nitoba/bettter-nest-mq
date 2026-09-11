import 'reflect-metadata'
import { Injectable } from '@nestjs/common'
import { z } from 'zod'
import { Job, Queue, QueueService, Schedule } from 'better-nest-mq'
import { zodCodec } from 'better-nest-mq/zod'

export const ValueSchema = z.union([
  z.string(),
  z.number(),
  z.boolean(),
  z.null(),
  z.array(z.string()),
  z.object({ text: z.string() })
])
const timestamp = z.codec(z.iso.datetime(), z.date(), {
  decode: (text) => new Date(text),
  encode: (date) => date.toISOString()
})
@Injectable()
@Queue({ name: 'scheduled-public', connection: 'primary' })
export class ScheduledQueue extends QueueService {
  @Job({ name: 'echo', version: 1 })
  @Schedule({ key: 'regular', everyMs: 3_600_000, payload: 'scheduled' })
  readonly echo = this.job({ payload: ValueSchema, result: ValueSchema })
  @Job({ name: 'date', version: 1 })
  readonly date = this.job({
    payload: zodCodec(z.object({ timestamp })),
    result: zodCodec(z.object({ timestamp }))
  })
}
export const SchedulerEnvironment = z.object({
  MQ_TEST_DATABASE_URL: z.string().min(1),
  MQ_SCHEDULE_SCHEMA: z.string().regex(/^mq_schedule_[a-f0-9]+$/)
})
