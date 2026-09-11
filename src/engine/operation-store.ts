import { Layer } from 'better-effect'
import { JobStore } from 'better-effect-mq'
import type { JobStoreToken, FlowStoreV2 } from 'better-effect-mq'
import type { QueueDefinition } from '../contracts/queue-definition.ts'
import { namedStoreToken } from './connection-definition.ts'
import type { NamedStore } from './connection-definition.ts'
import { dispatchStore } from './controlled-store.ts'
import { flowReadStore, type FlowJobReads } from './flow-read-store.ts'

export type OperationStoreToken = JobStoreToken<`nestjs.operation/${string}`>
export type OperationStore = InstanceType<OperationStoreToken>
export function operationStoreToken(name: string): OperationStoreToken {
  const tag: `nestjs.operation/${string}` = `nestjs.operation/${name}`
  return JobStore.named(tag)
}
/** Raw identity is unchanged; these are runtime protocol/read views over the same physical store. */
export function operationStoreLayer(
  name: string,
  queues: readonly QueueDefinition[],
  reads?: FlowJobReads,
  flow: () => FlowStoreV2 | undefined = () => undefined
): Layer<OperationStore, NamedStore> {
  const raw = namedStoreToken(name)
  return Layer.gen(operationStoreToken(name), async function* () {
    const store = flowReadStore(yield* raw, reads, flow)
    return dispatchStore(
      store,
      queues.filter((queue) => queue.connection === name)
    )
  })
}
