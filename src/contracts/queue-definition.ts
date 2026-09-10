import { getJobMetadata, getOwnQueueMetadata } from './decorators.ts'
import { ContractDefinitionException } from './errors.ts'
import { JobContract } from './job-definition.ts'
import { resolveJobPolicy } from './policies.ts'
import type { JobPolicy, ResolvedJobPolicy } from './policies.ts'
import type { QueueService } from './queue-service.ts'

export interface JobIdentity {
  readonly connection: string
  readonly queue: string
  readonly name: string
  readonly version: number
  /** A JSON tuple avoids delimiter collisions and excludes refactorable class/property names. */
  readonly key: string
}

export interface RegisteredJob {
  readonly property: string
  readonly identity: JobIdentity
  readonly policy: ResolvedJobPolicy
  readonly contract: JobContract
}

export interface QueueDefinition {
  readonly name: string
  readonly connection: string
  readonly jobs: ReadonlyArray<RegisteredJob>
}

/** Inspect actual fields without evaluating getters or starting queue infrastructure. */
export function getQueueDefinition(queue: QueueService, defaults: JobPolicy = {}): QueueDefinition {
  const metadata = getOwnQueueMetadata(queue.constructor)
  if (metadata === undefined) {
    throw new ContractDefinitionException('Every concrete QueueService must declare its own @Queue identity')
  }
  const properties = getJobMetadata(queue)
  for (const key of Reflect.ownKeys(queue)) {
    const value = Object.getOwnPropertyDescriptor(queue, key)?.value
    if (value instanceof JobContract && properties.get(String(key))?.job === undefined) {
      throw new ContractDefinitionException(`Missing @Job on ${String(key)}`)
    }
  }

  const jobs: RegisteredJob[] = []
  const identities = new Set<string>()
  for (const [property, declaration] of properties) {
    if (declaration.job === undefined) throw new ContractDefinitionException(`Missing @Job on ${property}`)
    const descriptor = Object.getOwnPropertyDescriptor(queue, property)
    if (descriptor === undefined || descriptor.get !== undefined || descriptor.set !== undefined || !(descriptor.value instanceof JobContract)) {
      throw new ContractDefinitionException(`${property} must be an initialized job field, not an accessor or unrelated value`)
    }
    const job = declaration.job
    const key = JSON.stringify([metadata.connection, metadata.name, job.name, job.version])
    if (identities.has(key)) throw new ContractDefinitionException(`Duplicate job identity ${key}`)
    identities.add(key)
    jobs.push(Object.freeze({
      property,
      identity: Object.freeze({ connection: metadata.connection, queue: metadata.name, name: job.name, version: job.version, key }),
      policy: resolveJobPolicy(defaults, metadata.defaults, job.defaults, { retry: declaration.retry, timeoutMs: declaration.timeoutMs }),
      contract: descriptor.value
    }))
  }
  if (jobs.length === 0) throw new ContractDefinitionException(`Queue ${metadata.name} declares no jobs`)
  return Object.freeze({ name: metadata.name, connection: metadata.connection, jobs: Object.freeze(jobs) })
}
