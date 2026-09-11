import 'reflect-metadata'
import type { Type } from '@nestjs/common'
import type { JobContract } from '../contracts/job-definition.ts'
import { ContractDefinitionException } from '../contracts/errors.ts'
import { requireInteger, requireName } from '../contracts/policies.ts'
import { QueueService } from '../contracts/queue-service.ts'
import type { ProcessOptions, WorkerOptions } from './types.ts'

const WORKER = Symbol('mq.worker')
const PROCESSORS = Symbol('mq.processors')
const PARAMETERS = Symbol('mq.parameters')

type JobProperty<Queue extends QueueService> = { [Key in keyof Queue]: Queue[Key] extends JobContract ? Key : never }[keyof Queue] & string
export type JobParameter = 'payload' | 'context'
export interface ProcessMetadata {
  readonly queue: Type<QueueService>
  readonly property: string
  readonly options: ProcessOptions
}

export function Worker(options: WorkerOptions): ClassDecorator {
  requireName(options.name, 'worker.name')
  for (const [name, value] of Object.entries({ concurrency: options.concurrency, leaseDurationMs: options.leaseDurationMs, heartbeatIntervalMs: options.heartbeatIntervalMs, stalledIntervalMs: options.stalledIntervalMs, pollIntervalMs: options.pollIntervalMs })) {
    if (value !== undefined) requireInteger(value, `worker.${name}`, 1)
  }
  if (options.maxStalledCount !== undefined) requireInteger(options.maxStalledCount, 'worker.maxStalledCount')
  const lease = options.leaseDurationMs ?? 30_000
  if ((options.heartbeatIntervalMs ?? Math.max(1, Math.floor(lease / 3))) >= lease) throw new ContractDefinitionException('Worker heartbeat must be shorter than its lease')
  const snapshot = Object.freeze({ ...options })
  return (target) => {
    if (Reflect.hasOwnMetadata(WORKER, target)) throw new ContractDefinitionException('Duplicate @Worker decorator')
    Reflect.defineMetadata(WORKER, snapshot, target)
  }
}

export function Process<Queue extends QueueService>(queue: Type<Queue>, property: JobProperty<Queue>, options: ProcessOptions = {}): MethodDecorator {
  if (!(queue.prototype instanceof QueueService)) throw new ContractDefinitionException('@Process requires a QueueService class')
  if (options.concurrency !== undefined) requireInteger(options.concurrency, 'process.concurrency', 1)
  const metadata: ProcessMetadata = Object.freeze({ queue, property, options: Object.freeze({ ...options }) })
  return (target, method, descriptor) => {
    if (method !== String(method) || !(descriptor.value instanceof Function)) throw new ContractDefinitionException('@Process requires a string-named method')
    const methods: ReadonlyMap<string, ProcessMetadata> = Reflect.getOwnMetadata(PROCESSORS, target) ?? new Map()
    if (methods.has(String(method))) throw new ContractDefinitionException('Duplicate @Process decorator')
    const next = new Map(methods)
    next.set(String(method), metadata)
    Reflect.defineMetadata(PROCESSORS, next, target)
  }
}

function parameter(kind: JobParameter): ParameterDecorator {
  return (target, method, index) => {
    if (method === undefined || method !== String(method)) throw new ContractDefinitionException('MQ parameters are only supported on named methods')
    const parameters: ReadonlyMap<string, ReadonlyMap<number, JobParameter>> = Reflect.getOwnMetadata(PARAMETERS, target) ?? new Map()
    const current = new Map(parameters.get(String(method)))
    if (current.has(index)) throw new ContractDefinitionException('Duplicate MQ parameter decorator')
    current.set(index, kind)
    const next = new Map(parameters)
    next.set(String(method), current)
    Reflect.defineMetadata(PARAMETERS, next, target)
  }
}

export function JobData(): ParameterDecorator { return parameter('payload') }
export function JobContext(): ParameterDecorator { return parameter('context') }
export function workerMetadata<Target extends object>(target: Target): WorkerOptions | undefined { return Reflect.getOwnMetadata(WORKER, target) }

export function processMetadata<Target extends object>(target: Target): ReadonlyMap<string, ProcessMetadata> {
  const hierarchy: object[] = []
  let current = target
  while (current !== null) { hierarchy.push(current); current = Object.getPrototypeOf(current) }
  const merged = new Map<string, ProcessMetadata>()
  for (const prototype of hierarchy.reverse()) {
    const methods: ReadonlyMap<string, ProcessMetadata> | undefined = Reflect.getOwnMetadata(PROCESSORS, prototype)
    for (const [name, metadata] of methods ?? []) merged.set(name, metadata)
  }
  return merged
}

export function parameterMetadata<Target extends object>(target: Target, method: string): ReadonlyMap<number, JobParameter> {
  let current = target
  while (current !== null) {
    if (Object.getOwnPropertyDescriptor(current, method) !== undefined) {
      // Parameter annotations belong to the implementation actually invoked. An override
      // must not accidentally borrow a differently ordered parameter map from its base class.
      const parameters: ReadonlyMap<string, ReadonlyMap<number, JobParameter>> | undefined = Reflect.getOwnMetadata(PARAMETERS, current)
      return parameters?.get(method) ?? new Map()
    }
    current = Object.getPrototypeOf(current)
  }
  return new Map()
}
