import 'reflect-metadata'
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { Injectable, Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import {
  Job,
  MqConfiguration,
  MqModule,
  MqRegistry,
  Queue,
  QueueService,
  type MqModuleOptions
} from 'better-nest-mq'

assert.throws(() => createRequire(import.meta.url).resolve('zod'), { code: 'MODULE_NOT_FOUND' })

const text: StandardSchemaV1<string> = {
  '~standard': {
    version: 1,
    vendor: 'consumer',
    validate: (value) =>
      value === 'message' ? { value: 'message' } : { issues: [{ message: 'Expected message' }] }
  }
}

@Injectable()
@Queue({ name: 'external' })
class ExternalQueue extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: text, result: text })
}

@Module({ imports: [MqModule.forFeature([ExternalQueue])], exports: [MqModule] })
class ExternalMessagingModule {}

const options = {
  shutdown: { gracePeriodMs: 125, abortAfterGracePeriod: false },
  defaults: { priority: 5 }
} satisfies MqModuleOptions

// No explicit @Inject: the consumer must preserve normal Nest constructor metadata.
@Injectable()
class Consumer {
  constructor(
    readonly configuration: MqConfiguration,
    readonly registry: MqRegistry,
    readonly queue: ExternalQueue
  ) {}
}

@Module({
  imports: [MqModule.forRoot(options), ExternalMessagingModule],
  providers: [Consumer]
})
class ApplicationModule {}

const app = await NestFactory.createApplicationContext(ApplicationModule, { logger: false })
try {
  const consumer = app.get(Consumer)
  assert.deepEqual(consumer.configuration.options.shutdown, options.shutdown)
  assert.equal(Object.isFrozen(consumer.configuration.options), true)
  assert.equal(consumer.registry.jobs().length, 1)
  assert.equal(consumer.registry.jobs()[0]?.policy.priority, 5)
  assert.equal(
    await consumer.queue.echo.decodePayload(await consumer.queue.echo.encodePayload('message')),
    'message'
  )
} finally {
  await app.close()
}
assert.equal(app.get(MqRegistry).jobs().length, 0)

@Module({
  imports: [MqModule.forRootAsync({ useFactory: async () => options }), ExternalMessagingModule]
})
class AsyncApplicationModule {}

const asyncApp = await NestFactory.createApplicationContext(AsyncApplicationModule, {
  logger: false
})
try {
  assert.deepEqual(asyncApp.get(MqConfiguration).options.shutdown, options.shutdown)
  assert.equal(asyncApp.get(MqRegistry).jobs().length, 1)
} finally {
  await asyncApp.close()
}
console.log(
  'External consumer: real DI, module re-exports, contracts, lifecycle and Zod-free root passed'
)
