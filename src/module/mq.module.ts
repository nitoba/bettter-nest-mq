import { Module } from '@nestjs/common'

import { MqConfiguration } from './mq.configuration.ts'
import { ConfigurableModuleClass } from './mq-module.definition.ts'

/** Nest module foundation. Registration never creates connections or starts consumers. */
@Module({ providers: [MqConfiguration], exports: [MqConfiguration] })
export class MqModule extends ConfigurableModuleClass {}
