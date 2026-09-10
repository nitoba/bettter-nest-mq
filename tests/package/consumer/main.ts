import 'reflect-metadata'
import assert from 'node:assert/strict'
import { Injectable, Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { MqConfiguration, MqModule, type MqModuleOptions } from 'better-nest-mq'

const options = {
  shutdown: { gracePeriodMs: 125, abortAfterGracePeriod: false }
} satisfies MqModuleOptions

// No explicit @Inject: consumer compilation must preserve normal Nest constructor metadata.
@Injectable()
class Consumer {
  constructor(readonly configuration: MqConfiguration) {}
}

@Module({ imports: [MqModule.forRoot(options)], providers: [Consumer] })
class ApplicationModule {}

const app = await NestFactory.createApplicationContext(ApplicationModule, { logger: false })
try {
  assert.deepEqual(app.get(Consumer).configuration.options, options)
  assert.equal(Object.isFrozen(app.get(MqConfiguration).options.shutdown), true)
} finally {
  await app.close()
}

@Module({
  imports: [MqModule.forRootAsync({ useFactory: async () => options })]
})
class AsyncApplicationModule {}

const asyncApp = await NestFactory.createApplicationContext(AsyncApplicationModule, {
  logger: false
})
try {
  assert.deepEqual(asyncApp.get(MqConfiguration).options, options)
} finally {
  await asyncApp.close()
}
console.log('External consumer: DI, declarations, ESM and lifecycle passed')
