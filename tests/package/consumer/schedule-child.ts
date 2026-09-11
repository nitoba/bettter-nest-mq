import 'reflect-metadata'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { z } from 'zod'
import { MqModule, MqSchedulesService } from 'better-nest-mq'
import { postgres } from 'better-nest-mq/postgres'
import { ScheduledQueue, SchedulerEnvironment } from './schedule-contracts.js'

const environment = SchedulerEnvironment.parse(process.env)
@Module({
  imports: [
    MqModule.forRoot({
      connections: {
        primary: postgres({
          connectionString: environment.MQ_TEST_DATABASE_URL,
          schema: environment.MQ_SCHEDULE_SCHEMA,
          namespace: 'schedules',
          schedules: true
        })
      },
      execution: { workers: false, scheduler: true, outboxPublisher: false },
      schedules: { sweepIntervalMs: 3_600_000 }
    }),
    MqModule.forFeature([ScheduledQueue])
  ]
})
class SchedulerModule {}
const app = await NestFactory.createApplicationContext(SchedulerModule, {
  logger: false,
  abortOnError: false
})
const service = app.get(MqSchedulesService)
await service.sweep()
const command = z.object({ id: z.string(), type: z.enum(['sweep', 'stop']) })
let finish: () => void = () => {
  throw new Error('Stop gate was not initialized')
}
const stopped = new Promise<void>((resolve) => {
  finish = resolve
})
let tail = Promise.resolve()
process.on('message', (message) => {
  const parsed = command.safeParse(message)
  if (!parsed.success) return
  const value = parsed.data
  tail = tail.then(async () => {
    if (value.type === 'stop') {
      finish()
      return
    }
    try {
      await service.sweep()
      process.send?.({ id: value.id, type: 'done' })
    } catch (cause) {
      process.send?.({
        id: value.id,
        type: 'error',
        message: cause instanceof Error ? cause.message : 'sweep failed'
      })
    }
  })
})
process.once('disconnect', () => finish())
process.send?.({ type: 'ready', pid: process.pid })
try {
  await stopped
  await tail
} finally {
  await app.close()
  process.disconnect?.()
}
