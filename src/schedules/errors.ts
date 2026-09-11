export type SchedulePhase = 'definition' | 'unavailable' | 'identity' | 'missing' | 'drift' | 'mode' | 'operation'

export class MqScheduleException extends Error {
  readonly code = 'MQ_SCHEDULE'
  constructor(readonly phase: SchedulePhase, message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = 'MqScheduleException'
  }
}
