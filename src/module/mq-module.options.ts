import type { MqConnectionMap } from '../connections/connection.ts'
import type { MqControlsOptions } from '../controls/types.ts'
import type { JobPolicy, ResolvedJobPolicy } from '../contracts/policies.ts'

export interface MqShutdownOptions {
  readonly gracePeriodMs?: number
  readonly abortAfterGracePeriod?: boolean
}
export interface MqExecutionOptions {
  readonly workers?: boolean
}

export interface MqModuleOptions {
  readonly shutdown?: MqShutdownOptions
  readonly defaults?: JobPolicy
  readonly connections?: MqConnectionMap
  readonly execution?: MqExecutionOptions
  readonly controls?: MqControlsOptions
}

export interface MqOptionsFactory {
  createMqOptions(): MqModuleOptions | Promise<MqModuleOptions>
}
export interface MqResolvedOptions {
  readonly shutdown: Readonly<Required<MqShutdownOptions>>
  readonly defaults: ResolvedJobPolicy
  readonly connections: MqConnectionMap | undefined
  readonly execution: Readonly<Required<MqExecutionOptions>>
  readonly controls: Readonly<Required<MqControlsOptions>>
}
