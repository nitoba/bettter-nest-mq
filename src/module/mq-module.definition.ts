import { ConfigurableModuleBuilder } from '@nestjs/common'
import { DiscoveryModule } from '@nestjs/core'
import type { MqConnectionMonitor } from '../connections/connection.ts'
import { MqConnectionsService } from '../connections/mq-connections.service.ts'
import { CONNECTION_MONITOR } from '../connections/tokens.ts'
import { MqQueueControlsService, QUEUE_CONTROLS_MONITOR } from '../controls/service.ts'
import type { QueueControlsMonitor } from '../controls/types.ts'
import { MqEngineHost } from '../engine/mq-engine.host.ts'
import { MqWorkersService, WORKER_MONITOR } from '../workers/mq-workers.service.ts'
import type { WorkerMonitor } from '../workers/types.ts'
import type { MqModuleOptions } from './mq-module.options.ts'
import { MqConfiguration } from './mq.configuration.ts'
import { MqRegistry } from './mq.registry.ts'
import { MODULE_OPTIONS_TOKEN } from './mq.tokens.ts'

export const { ConfigurableModuleClass } = new ConfigurableModuleBuilder<MqModuleOptions>({
  moduleName: 'Mq',
  optionsInjectionToken: MODULE_OPTIONS_TOKEN
})
  .setClassMethodName('forRoot')
  .setFactoryMethodName('createMqOptions')
  .setExtras({ isGlobal: false }, (definition, extras) => ({
    ...definition,
    global: extras.isGlobal,
    imports: [...(definition.imports ?? []), DiscoveryModule],
    providers: [
      ...(definition.providers ?? []),
      MqConfiguration,
      MqRegistry,
      MqEngineHost,
      {
        provide: CONNECTION_MONITOR,
        inject: [MqEngineHost],
        useFactory: (host: MqEngineHost): MqConnectionMonitor => host.session
      },
      {
        provide: WORKER_MONITOR,
        inject: [MqEngineHost],
        useFactory: (host: MqEngineHost): WorkerMonitor => host.session
      },
      {
        provide: QUEUE_CONTROLS_MONITOR,
        inject: [MqEngineHost],
        useFactory: (host: MqEngineHost): QueueControlsMonitor => host.controls
      },
      MqConnectionsService,
      MqWorkersService,
      MqQueueControlsService
    ],
    exports: [
      ...(definition.exports ?? []),
      MqConfiguration,
      MqRegistry,
      MqConnectionsService,
      MqWorkersService,
      MqQueueControlsService
    ]
  }))
  .build()
