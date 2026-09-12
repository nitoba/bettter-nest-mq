import type { EventLayerFactory } from './event-plan.ts'
import type { FlowJobReads } from './flow-read-store.ts'
import type { FlowLayerFactory } from './flow-plan.ts'
import type { ScheduleLayerFactory } from './schedule-plan.ts'
import type { OutboxLayerFactory } from './outbox-plan.ts'
import { JobStore } from 'better-effect-mq'
import type { JobStoreToken } from 'better-effect-mq'
import type { Layer } from 'better-effect'

import { connectionBrand } from '../connections/connection.ts'
import type {
  MqCapability,
  MqConnection,
  MqConnectionMap,
  MqConnectionOwnership
} from '../connections/connection.ts'
import { MqConnectionException } from '../connections/errors.ts'
import { requireName } from '../contracts/policies.ts'

export type NamedStoreToken = JobStoreToken<`nestjs/${string}`>
export type NamedStore = InstanceType<NamedStoreToken>

export interface AcquiredConnection {
  readonly events?: EventLayerFactory
  readonly flowReads?: (name: string) => FlowJobReads
  readonly flows?: FlowLayerFactory
  readonly schedules?: ScheduleLayerFactory
  readonly outbox?: OutboxLayerFactory
  readonly layer: Layer<NamedStore, never>
  /** Release facade-owned resources after the runtime has released adapter resources. */
  readonly release?: () => Promise<void>
}

interface ConnectionDefinition {
  readonly boundary: object | string
  readonly scope: string
  readonly requirements: ReadonlyArray<MqCapability>
  readonly acquire: (token: NamedStoreToken) => AcquiredConnection | Promise<AcquiredConnection>
}

interface ConnectionDefinitionOptions {
  readonly adapter: string
  readonly ownership: MqConnectionOwnership
  readonly boundary: object | string
  readonly scope: string
  readonly requirements?: ReadonlyArray<MqCapability>
}

// Only inert configuration lives here. Each factory acquires new resources for its owning context.
const definitions = new WeakMap<MqConnection, ConnectionDefinition>()

export function defineConnection(
  options: ConnectionDefinitionOptions,
  acquire: ConnectionDefinition['acquire']
): MqConnection {
  const connection = Object.freeze<MqConnection>({
    [connectionBrand]: true,
    adapter: options.adapter,
    ownership: options.ownership
  })
  definitions.set(
    connection,
    Object.freeze({
      boundary: options.boundary,
      scope: options.scope,
      requirements: Object.freeze([...(options.requirements ?? [])]),
      acquire
    })
  )
  return connection
}

export function connectionDefinition(connection: MqConnection, name: string): ConnectionDefinition {
  const definition = definitions.get(connection)
  if (definition === undefined) throw new MqConnectionException(name, 'configuration')
  return definition
}

export function copyConnections(
  connections: MqConnectionMap | undefined
): MqConnectionMap | undefined {
  if (connections === undefined) return undefined
  const entries = Object.entries(connections)
  for (const [name, connection] of entries) {
    requireName(name, 'connection.name')
    connectionDefinition(connection, name)
  }
  return Object.freeze(Object.fromEntries(entries))
}

export function namedStoreToken(name: string): NamedStoreToken {
  const qualified: `nestjs/${string}` = `nestjs/${name}`
  return JobStore.named(qualified)
}
