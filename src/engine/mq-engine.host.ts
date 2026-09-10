import { Inject, Injectable, type OnApplicationBootstrap, type OnModuleDestroy } from '@nestjs/common'

import { MqConfiguration } from '../module/mq.configuration.ts'
import { MqRegistry } from '../module/mq.registry.ts'
import { EngineSession } from './engine-session.ts'

/** Private Nest lifecycle owner. Never exported by the package entry point. */
@Injectable()
export class MqEngineHost implements OnApplicationBootstrap, OnModuleDestroy {
  readonly session: EngineSession

  constructor(
    @Inject(MqConfiguration) configuration: MqConfiguration,
    @Inject(MqRegistry) private readonly registry: MqRegistry
  ) {
    this.session = new EngineSession(configuration.options.connections, configuration.options.shutdown)
  }

  async onApplicationBootstrap(): Promise<void> {
    // Nest may run provider hooks concurrently. Do not rely on their incidental ordering.
    this.registry.initialize()
    await this.session.start(this.registry.queues())
  }

  onModuleDestroy(): Promise<void> {
    return this.session.close()
  }
}
