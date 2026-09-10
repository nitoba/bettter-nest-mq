import type { MqConnectionMap } from '../connections/connection.ts'
import type { JobPolicy, ResolvedJobPolicy } from '../contracts/policies.ts'

export interface MqShutdownOptions {
  readonly gracePeriodMs?: number
  readonly abortAfterGracePeriod?: boolean
}

export interface MqModuleOptions {
  readonly shutdown?: MqShutdownOptions
  readonly defaults?: JobPolicy
  /** Omit to register contracts without acquiring any storage resources. */
  readonly connections?: MqConnectionMap
}

export interface MqOptionsFactory {
  createMqOptions(): MqModuleOptions | Promise<MqModuleOptions>
}

export interface MqResolvedOptions {
  readonly shutdown: Readonly<Required<MqShutdownOptions>>
  readonly defaults: ResolvedJobPolicy
  readonly connections: MqConnectionMap | undefined
}
