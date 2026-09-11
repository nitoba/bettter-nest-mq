import { Inject, Injectable } from '@nestjs/common'
import { resolveControlsOptions } from '../controls/decorator.ts'
import { resolveOutboxOptions } from '../outbox/options.ts'
import { resolveJobPolicy } from '../contracts/policies.ts'
import { copyConnections } from '../engine/connection-definition.ts'
import { MODULE_OPTIONS_TOKEN } from './mq.tokens.ts'
import type { MqModuleOptions, MqResolvedOptions } from './mq-module.options.ts'

@Injectable()
export class MqConfiguration {
  readonly options: MqResolvedOptions
  constructor(@Inject(MODULE_OPTIONS_TOKEN) options: MqModuleOptions) {
    const gracePeriodMs = options.shutdown?.gracePeriodMs ?? 30_000
    if (!Number.isSafeInteger(gracePeriodMs) || gracePeriodMs < 0)
      throw new RangeError('shutdown.gracePeriodMs must be a non-negative safe integer')
    this.options = Object.freeze({
      shutdown: Object.freeze({
        gracePeriodMs,
        abortAfterGracePeriod: options.shutdown?.abortAfterGracePeriod ?? true
      }),
      defaults: resolveJobPolicy(options.defaults ?? {}),
      outbox: resolveOutboxOptions(options.outbox),
      controls: resolveControlsOptions(options.controls),
      connections: copyConnections(options.connections),
      execution: Object.freeze({
        workers: options.execution?.workers ?? true,
        outboxPublisher: options.execution?.outboxPublisher ?? true
      })
    })
  }
}
