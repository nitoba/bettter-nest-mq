import { MqFlowsService, FLOW_MONITOR } from '../flows/service.ts'
import type { FlowMonitor } from '../flows/types.ts'
import { ConfigurableModuleBuilder } from '@nestjs/common'
import { DiscoveryModule } from '@nestjs/core'
import type { MqConnectionMonitor } from '../connections/connection.ts'
import { MqConnectionsService } from '../connections/mq-connections.service.ts'
import { CONNECTION_MONITOR } from '../connections/tokens.ts'
import { MqQueueControlsService, QUEUE_CONTROLS_MONITOR } from '../controls/service.ts'
import type { QueueControlsMonitor } from '../controls/types.ts'
import { MqOutboxService } from '../outbox/service.ts'
import { bindOutboxService } from '../engine/outbox-coordinator.ts'
import { MqEngineHost } from '../engine/mq-engine.host.ts'
import { MqWorkersService, WORKER_MONITOR } from '../workers/mq-workers.service.ts'
import type { WorkerMonitor } from '../workers/types.ts'
import { MqSchedulesService, SCHEDULE_MONITOR } from '../schedules/service.ts'
import type { SchedulesMonitor } from '../schedules/types.ts'
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
        provide: MqOutboxService,
        inject: [MqEngineHost],
        useFactory: (host: MqEngineHost): MqOutboxService =>
          bindOutboxService(new MqOutboxService(host.outbox), host.outbox)
      },
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
      {
        provide: SCHEDULE_MONITOR,
        inject: [MqEngineHost],
        useFactory: (host: MqEngineHost): SchedulesMonitor => host.schedules
      },
      {
        provide: FLOW_MONITOR,
        inject: [MqEngineHost],
        useFactory: (host: MqEngineHost): FlowMonitor => host.flows
      },
      MqFlowsService,
      MqConnectionsService,
      MqWorkersService,
      MqQueueControlsService,
      MqSchedulesService
    ],
    exports: [
      ...(definition.exports ?? []),
      MqOutboxService,
      MqFlowsService,
      MqConfiguration,
      MqRegistry,
      MqConnectionsService,
      MqWorkersService,
      MqQueueControlsService,
      MqSchedulesService
    ]
  }))
  .build()
