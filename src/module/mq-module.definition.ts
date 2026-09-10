import { ConfigurableModuleBuilder } from '@nestjs/common'

import type { MqModuleOptions } from './mq-module.options.ts'

export const { ConfigurableModuleClass, MODULE_OPTIONS_TOKEN } =
  new ConfigurableModuleBuilder<MqModuleOptions>({ moduleName: 'Mq' })
    .setClassMethodName('forRoot')
    .setFactoryMethodName('createMqOptions')
    .setExtras({ isGlobal: false }, (definition, extras) => ({
      ...definition,
      global: extras.isGlobal
    }))
    .build()
