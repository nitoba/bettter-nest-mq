import { Layer } from 'better-effect'
import { JobEventStore } from 'better-effect-mq'
import type { JobEventStoreInstance, JobEventStoreToken } from 'better-effect-mq'
import { namedStoreToken } from './connection-definition.ts'
import type { NamedStoreToken, NamedStore } from './connection-definition.ts'
import { operationStoreToken } from './operation-store.ts'
import type { OperationStoreToken } from './operation-store.ts'

export type NamedEvents = JobEventStoreInstance<NamedStoreToken>
export type OperationEvents = JobEventStoreInstance<OperationStoreToken>
export type EventLayerFactory = (name: string) => Layer<NamedEvents, NamedStore>
export function eventToken(name: string): JobEventStoreToken<NamedStoreToken> {
  return JobEventStore.for(namedStoreToken(name))
}
export function operationEventToken(name: string): JobEventStoreToken<OperationStoreToken> {
  return JobEventStore.for(operationStoreToken(name))
}
/** Only an in-runtime alias: the physical event log uses the raw JobStore namespace. */
export function eventAlias(name: string): Layer<OperationEvents, NamedEvents> {
  return Layer.gen(operationEventToken(name), async function* () {
    return yield* eventToken(name)
  })
}
