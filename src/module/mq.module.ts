import { Module, type DynamicModule, type Type } from '@nestjs/common'

import type { QueueService } from '../contracts/queue-service.ts'
import { ConfigurableModuleClass } from './mq-module.definition.ts'

/** Root-only providers are added by forRoot/forRootAsync, not to every feature import. */
@Module({})
export class MqModule extends ConfigurableModuleClass {
  static forFeature(queues: ReadonlyArray<Type<QueueService>>): DynamicModule {
    const providers = [...new Set(queues)]
    return { module: MqModule, providers, exports: providers }
  }
}
