import { Layer } from 'better-effect'
import { MemoryJobStore } from 'better-effect-mq'

import type { MqCapability } from '../../src/connections/connection.ts'
import { defineConnection } from '../../src/engine/connection-definition.ts'

export interface ConnectionTrace {
  acquisitions: number
  layerReleases: number
  resourceReleases: number
  events: string[]
}

export interface TestConnectionOptions {
  readonly beforeAcquire?: Promise<void>
  readonly acquisitionFailure?: Error
  readonly releaseFailure?: Error
  readonly resourceFailure?: Error
  readonly requirements?: ReadonlyArray<MqCapability>
}

/** Explicit fixture backed by the actual upstream store, never a production fallback. */
export function memoryConnection(options: TestConnectionOptions = {}) {
  const trace: ConnectionTrace = { acquisitions: 0, layerReleases: 0, resourceReleases: 0, events: [] }
  const connection = defineConnection({
    adapter: 'memory',
    ownership: 'owned',
    boundary: trace,
    scope: 'test',
    requirements: options.requirements ?? []
  }, (token) => ({
    layer: Layer.scoped(token, async () => {
      trace.acquisitions += 1
      trace.events.push('acquire')
      await options.beforeAcquire
      if (options.acquisitionFailure !== undefined) throw options.acquisitionFailure
      return MemoryJobStore.make()
    }, () => {
      trace.layerReleases += 1
      trace.events.push('layer-release')
      if (options.releaseFailure !== undefined) throw options.releaseFailure
    }),
    release: async () => {
      trace.resourceReleases += 1
      trace.events.push('resource-release')
      if (options.resourceFailure !== undefined) throw options.resourceFailure
    }
  }))
  return { connection, trace }
}
