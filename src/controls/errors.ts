export type QueueControlsPhase =
  | 'configuration'
  | 'capability'
  | 'missing'
  | 'drift'
  | 'ownership'
  | 'mode'
  | 'operation'

export class QueueControlsException extends Error {
  readonly code = 'MQ_QUEUE_CONTROLS'
  constructor(
    readonly phase: QueueControlsPhase,
    message: string,
    options?: ErrorOptions
  ) {
    super(message, options)
    this.name = 'QueueControlsException'
  }
}
