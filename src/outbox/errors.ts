export type OutboxPhase =
  | 'configuration'
  | 'prepare'
  | 'unavailable'
  | 'read'
  | 'append'
  | 'conflict'
  | 'transaction'

export class MqOutboxException extends Error {
  readonly code = 'MQ_OUTBOX'
  constructor(
    readonly phase: OutboxPhase,
    message: string,
    options?: ErrorOptions
  ) {
    super(message, options)
    this.name = 'MqOutboxException'
  }
}
