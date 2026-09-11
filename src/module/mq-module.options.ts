import type { MqConnectionMap } from '../connections/connection.ts'
import type { JobPolicy, ResolvedJobPolicy } from '../contracts/policies.ts'

export interface MqShutdownOptions { readonly gracePeriodMs?: number; readonly abortAfterGracePeriod?: boolean }
export interface MqExecutionOptions { readonly workers?: boolean }

export interface MqModuleOptions {
  readonly shutdown?: MqShutdownOptions
  readonly defaults?: JobPolicy
  readonly connections?: MqConnectionMap
  /** Worker services run only when registered and enabled. Producers never require them. */
  readonly execution?: MqExecutionOptions
}

export interface MqOptionsFactory { createMqOptions(): MqModuleOptions | Promise<MqModuleOptions> }
export interface MqResolvedOptions {
  readonly shutdown: Readonly<Required<MqShutdownOptions>>
  readonly defaults: ResolvedJobPolicy
  readonly connections: MqConnectionMap | undefined
  readonly execution: Readonly<Required<MqExecutionOptions>>
}
