import { Module, type DynamicModule, type Type } from '@nestjs/common'
import { DiscoveryModule } from '@nestjs/core'

import type { QueueService } from '../contracts/queue-service.ts'
import { MqConfiguration } from './mq.configuration.ts'
import { MqRegistry } from './mq.registry.ts'
import { ConfigurableModuleClass } from './mq-module.definition.ts'

@Module({})
class MqFeatureModule {}

/** Registration exposes inert contracts and discovery, never connections or consumers. */
@Module({
  imports: [DiscoveryModule],
  providers: [MqConfiguration, MqRegistry],
  exports: [MqConfiguration, MqRegistry]
})
export class MqModule extends ConfigurableModuleClass {
  static forFeature(queues: ReadonlyArray<Type<QueueService>>): DynamicModule {
    const providers = [...new Set(queues)]
    return { module: MqFeatureModule, providers, exports: providers }
  }
}
