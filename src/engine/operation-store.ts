import { Layer } from 'better-effect'
import { JobStore } from 'better-effect-mq'
import type { JobStoreToken } from 'better-effect-mq'
import type { QueueDefinition } from '../contracts/queue-definition.ts'
import { namedStoreToken } from './connection-definition.ts'
import type { NamedStore } from './connection-definition.ts'
import { dispatchStore } from './controlled-store.ts'

export type OperationStoreToken = JobStoreToken<`nestjs.operation/${string}`>
export type OperationStore = InstanceType<OperationStoreToken>

export function operationStoreToken(name: string): OperationStoreToken {
  const tag: `nestjs.operation/${string}` = `nestjs.operation/${name}`
  return JobStore.named(tag)
}

/** Keep the raw token/namespace unchanged; the additional token is an in-runtime protocol view. */
export function operationStoreLayer(name: string, queues: readonly QueueDefinition[]): Layer<OperationStore, NamedStore> {
  const raw = namedStoreToken(name)
  return Layer.gen(operationStoreToken(name), async function* () {
    return dispatchStore(yield* raw, queues.filter((queue) => queue.connection === name))
  })
}
