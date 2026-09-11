import type { MqScheduleOptions } from '../schedules/types.ts'
import type { MqConnectionMap } from '../connections/connection.ts'
import type { MqControlsOptions } from '../controls/types.ts'
import type { JobPolicy, ResolvedJobPolicy } from '../contracts/policies.ts'
import type { MqOutboxOptions } from '../outbox/types.ts'

export interface MqShutdownOptions {
  readonly gracePeriodMs?: number
  readonly abortAfterGracePeriod?: boolean
}
export interface MqExecutionOptions {
  readonly scheduler?: boolean
  readonly workers?: boolean
  readonly outboxPublisher?: boolean
}
export interface MqModuleOptions {
  readonly schedules?: MqScheduleOptions
  readonly shutdown?: MqShutdownOptions
  readonly defaults?: JobPolicy
  readonly connections?: MqConnectionMap
  readonly execution?: MqExecutionOptions
  readonly controls?: MqControlsOptions
  readonly outbox?: MqOutboxOptions
}
export interface MqOptionsFactory {
  createMqOptions(): MqModuleOptions | Promise<MqModuleOptions>
}
export interface MqResolvedOptions {
  readonly schedules: Readonly<Required<MqScheduleOptions>>
  readonly shutdown: Readonly<Required<MqShutdownOptions>>
  readonly defaults: ResolvedJobPolicy
  readonly connections: MqConnectionMap | undefined
  readonly execution: Readonly<Required<MqExecutionOptions>>
  readonly controls: Readonly<Required<MqControlsOptions>>
  readonly outbox: Readonly<Required<MqOutboxOptions>>
}
