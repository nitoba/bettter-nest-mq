import { Inject, Injectable } from '@nestjs/common'

import { resolveJobPolicy } from '../contracts/policies.ts'
import { MODULE_OPTIONS_TOKEN } from './mq.tokens.ts'
import type { MqModuleOptions, MqResolvedOptions } from './mq-module.options.ts'

/** Immutable, application-context-local configuration; it owns no external resources. */
@Injectable()
export class MqConfiguration {
  readonly options: MqResolvedOptions

  constructor(@Inject(MODULE_OPTIONS_TOKEN) options: MqModuleOptions) {
    const gracePeriodMs = options.shutdown?.gracePeriodMs ?? 30_000
    if (!Number.isSafeInteger(gracePeriodMs) || gracePeriodMs < 0) {
      throw new RangeError('shutdown.gracePeriodMs must be a non-negative safe integer')
    }
    this.options = Object.freeze({
      shutdown: Object.freeze({
        gracePeriodMs,
        abortAfterGracePeriod: options.shutdown?.abortAfterGracePeriod ?? true
      }),
      defaults: resolveJobPolicy(options.defaults ?? {})
    })
  }
}
