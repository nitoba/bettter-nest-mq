/** Shutdown policy reserved for the engine host; this bootstrap does not start workers. */
export interface MqShutdownOptions {
  /** Non-negative, safe integer in milliseconds. Defaults to 30 seconds. */
  readonly gracePeriodMs?: number
  /** Request cooperative cancellation after the grace period. Defaults to true. */
  readonly abortAfterGracePeriod?: boolean
}

/** Initial module configuration. Connection and worker options arrive with the engine bridge. */
export interface MqModuleOptions {
  readonly shutdown?: MqShutdownOptions
}

export interface MqOptionsFactory {
  createMqOptions(): MqModuleOptions | Promise<MqModuleOptions>
}

export interface MqResolvedOptions {
  readonly shutdown: Readonly<Required<MqShutdownOptions>>
}
