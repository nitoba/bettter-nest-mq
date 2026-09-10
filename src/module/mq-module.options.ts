import type { JobPolicy, ResolvedJobPolicy } from '../contracts/policies.ts'

/** Shutdown policy reserved for the engine host; contracts do not start workers. */
export interface MqShutdownOptions {
  readonly gracePeriodMs?: number
  readonly abortAfterGracePeriod?: boolean
}

export interface MqModuleOptions {
  readonly shutdown?: MqShutdownOptions
  readonly defaults?: JobPolicy
}

export interface MqOptionsFactory {
  createMqOptions(): MqModuleOptions | Promise<MqModuleOptions>
}

export interface MqResolvedOptions {
  readonly shutdown: Readonly<Required<MqShutdownOptions>>
  readonly defaults: ResolvedJobPolicy
}
