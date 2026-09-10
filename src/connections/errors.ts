import type { MqEngineState } from './connection.ts'

export type MqConnectionPhase = 'configuration' | 'acquire' | 'protocol' | 'probe' | 'close' | 'migrate'

export class MqConnectionException extends Error {
  readonly code = 'MQ_CONNECTION_FAILURE'

  constructor(readonly connection: string, readonly phase: MqConnectionPhase, options?: ErrorOptions) {
    super(`MQ connection ${JSON.stringify(connection)} failed during ${phase}`, options)
    this.name = 'MqConnectionException'
  }
}

export class MqEngineStateException extends Error {
  readonly code = 'MQ_ENGINE_STATE'

  constructor(readonly state: MqEngineState, readonly operation: string) {
    super(`Cannot ${operation} while the MQ engine is ${state}`)
    this.name = 'MqEngineStateException'
  }
}
