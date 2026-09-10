import { ConfigurableModuleBuilder } from '@nestjs/common'
import { DiscoveryModule } from '@nestjs/core'

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
    providers: [...(definition.providers ?? []), MqConfiguration, MqRegistry],
    exports: [...(definition.exports ?? []), MqConfiguration, MqRegistry]
  }))
  .build()
