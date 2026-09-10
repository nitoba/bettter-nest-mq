export const connectionBrand: unique symbol = Symbol('mq.connection')

export type MqEngineState =
  | 'idle'
  | 'disabled'
  | 'starting'
  | 'ready'
  | 'stopping'
  | 'closed'
  | 'failed'
export type MqConnectionOwnership = 'owned' | 'borrowed'

export interface MqStoreCapabilities {
  readonly queueFilteredNotifications: boolean
  readonly nativeBatchEnqueue: boolean
  readonly nativeBatchClaim: boolean
  readonly metadataIndex: 'none' | 'residual' | 'indexed'
  readonly transactionalEnqueue: boolean
  readonly durableChangeFeed: boolean
  readonly globalConcurrency: boolean
  readonly rateLimiting: boolean
}

export type MqCapability = Exclude<keyof MqStoreCapabilities, 'metadataIndex'>

/** Opaque, inert adapter configuration. Construct it through a supported integration factory. */
export interface MqConnection {
  readonly [connectionBrand]: true
  readonly adapter: string
  readonly ownership: MqConnectionOwnership
}

export type MqConnectionMap = Readonly<Record<string, MqConnection>>

/** Detached diagnostics: no credentials, pools, engine tokens or executable configuration. */
export interface MqConnectionSnapshot {
  readonly name: string
  readonly adapter: string
  readonly adapterVersion: string
  readonly protocolVersion: number
  readonly layoutVersion: number | string
  readonly ownership: MqConnectionOwnership
  readonly capabilities: MqStoreCapabilities
}

/** Promise-only boundary between public Nest services and the private host. */
export interface MqConnectionMonitor {
  readonly state: MqEngineState
  connections(): ReadonlyArray<MqConnectionSnapshot>
  probe(name: string): Promise<MqConnectionSnapshot>
}
