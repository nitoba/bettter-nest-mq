import 'reflect-metadata'

import { ContractDefinitionException } from './errors.ts'
import { copyJobPolicy, copyRetry, requireInteger, requireName } from './policies.ts'
import type { JobPolicy, RetryOptions } from './policies.ts'
import { QueueService } from './queue-service.ts'

const QUEUE = Symbol('mq.queue')
const JOBS = Symbol('mq.jobs')

export interface QueueOptions {
  readonly name: string
  readonly connection?: string
  readonly defaults?: JobPolicy
}

export interface JobDecoratorOptions {
  readonly name: string
  readonly version: number
  readonly defaults?: JobPolicy
}

export interface QueueMetadata {
  readonly name: string
  readonly connection: string
  readonly defaults: JobPolicy
}

interface JobIdentityMetadata {
  readonly name: string
  readonly version: number
  readonly defaults: JobPolicy
}

export interface JobMetadata {
  readonly job?: JobIdentityMetadata
  readonly retry?: RetryOptions
  readonly timeoutMs?: number
}

export function getOwnQueueMetadata<Target extends object>(
  target: Target
): QueueMetadata | undefined {
  return Reflect.getOwnMetadata(QUEUE, target)
}

function ownJobs<Target extends object>(target: Target): ReadonlyMap<string, JobMetadata> {
  const metadata: ReadonlyMap<string, JobMetadata> | undefined = Reflect.getOwnMetadata(
    JOBS,
    target
  )
  return metadata ?? new Map<string, JobMetadata>()
}

function decorateProperty(kind: keyof JobMetadata, patch: JobMetadata): PropertyDecorator {
  return (target, key) => {
    if (!(target instanceof QueueService) || key !== String(key)) {
      throw new ContractDefinitionException(
        'Job decorators require a string-named instance property of QueueService'
      )
    }
    const property = String(key)
    const current = ownJobs(target)
    const previous = current.get(property)
    if (previous?.[kind] !== undefined) {
      throw new ContractDefinitionException(`Duplicate ${kind} decorator on ${property}`)
    }
    const next = new Map(current)
    next.set(property, Object.freeze({ ...previous, ...patch }))
    Reflect.defineMetadata(JOBS, next, target)
  }
}

export function Queue(options: QueueOptions): ClassDecorator {
  requireName(options.name, 'queue.name')
  const connection = options.connection ?? 'default'
  requireName(connection, 'queue.connection')
  const metadata: QueueMetadata = Object.freeze({
    name: options.name,
    connection,
    defaults: copyJobPolicy(options.defaults ?? {})
  })
  return (target) => {
    if (!(target.prototype instanceof QueueService)) {
      throw new ContractDefinitionException('@Queue requires a QueueService subclass')
    }
    if (getOwnQueueMetadata(target) !== undefined)
      throw new ContractDefinitionException('Duplicate @Queue decorator')
    Reflect.defineMetadata(QUEUE, metadata, target)
  }
}

export function Job(options: JobDecoratorOptions): PropertyDecorator {
  requireName(options.name, 'job.name')
  requireInteger(options.version, 'job.version', 1)
  return decorateProperty('job', {
    job: Object.freeze({
      name: options.name,
      version: options.version,
      defaults: copyJobPolicy(options.defaults ?? {})
    })
  })
}

export function Retry(options: RetryOptions): PropertyDecorator {
  return decorateProperty('retry', { retry: copyRetry(options) })
}

export function JobTimeout(timeoutMs: number): PropertyDecorator {
  requireInteger(timeoutMs, 'timeoutMs')
  return decorateProperty('timeoutMs', { timeoutMs })
}

export function getJobMetadata(queue: QueueService): ReadonlyMap<string, JobMetadata> {
  const chain: object[] = []
  let current = Object.getPrototypeOf(queue)
  while (current !== null) {
    chain.push(current)
    current = Object.getPrototypeOf(current)
  }
  const merged = new Map<string, JobMetadata>()
  for (const prototype of chain.reverse()) {
    for (const [property, metadata] of ownJobs(prototype)) {
      merged.set(property, Object.freeze({ ...merged.get(property), ...metadata }))
    }
  }
  return merged
}
