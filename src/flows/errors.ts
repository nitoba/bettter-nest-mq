export type FlowPhase = 'definition' | 'unavailable' | 'identity' | 'closed' | 'operation'

export class MqFlowException extends Error {
  readonly code = 'MQ_FLOW'
  constructor(
    readonly phase: FlowPhase,
    message: string,
    options?: ErrorOptions
  ) {
    super(message, options)
    this.name = 'MqFlowException'
  }
}
