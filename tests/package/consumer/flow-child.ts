import 'reflect-metadata'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { Pool } from 'pg'
import { MqModule } from 'better-nest-mq'
import { postgres } from 'better-nest-mq/postgres'
import {
  FlowEnvironment,
  FLOW_POOL,
  FLOW_SETTINGS,
  FlowQueue,
  FlowWorkers
} from './flow-contracts.js'

const environment = FlowEnvironment.parse(process.env)
const pool = new Pool({ connectionString: environment.MQ_TEST_DATABASE_URL, max: 8 })
@Module({
  imports: [
    MqModule.forRoot({
      connections: {
        primary: postgres({
          pool,
          schema: environment.MQ_FLOW_SCHEMA,
          namespace: 'flows',
          flows: true
        })
      },
      shutdown: { gracePeriodMs: 50 },
      execution: { scheduler: false, outboxPublisher: false }
    }),
    MqModule.forFeature([FlowQueue])
  ],
  providers: [
    { provide: FLOW_POOL, useValue: pool },
    {
      provide: FLOW_SETTINGS,
      useValue: { schema: environment.MQ_FLOW_SCHEMA, hold: environment.MQ_FLOW_HOLD === 'true' }
    },
    ...FlowWorkers
  ]
})
class FlowWorkerModule {}
const app = await NestFactory.createApplicationContext(FlowWorkerModule, {
  logger: false,
  abortOnError: false
})
let stop: () => void = () => {
  throw new Error('Shutdown gate has not initialized')
}
const stopped = new Promise<void>((resolve) => {
  stop = resolve
})
process.once('message', () => stop())
process.once('disconnect', () => stop())
process.send?.({ type: 'ready', pid: process.pid })
try {
  await stopped
} finally {
  await app.close()
  await pool.end()
  process.disconnect?.()
}
